import re
import json
import time
import asyncio
import curl_cffi
from typing import Any
from curl_cffi import AsyncSession
from curl_cffi.requests.models import Response
from curl_cffi.requests.session import ProxySpec
from curl_cffi.requests.exceptions import RequestException # Added RequestException
from lxml import html
from aiolimiter import AsyncLimiter
from urllib.parse import urlencode, urlparse, parse_qs
from common import get_authentication_token, decode_base64_string
from common import encode_urlsafe_base64, calculate_md5_hash
from common import generate_auth_token, regenerate_api_token

async def fetch_page_data(
    session: AsyncSession,
    page: int,
    city_id: str,
    auth_token: str,
    ref_url: str,
    encrypted_input: str,
    cookies: dict[str, Any],
    limiter: AsyncLimiter,
    # proxies: ProxySpec
) -> Response | None:
    """Asynchronously fetches data for a single page, respecting the rate limit."""

    async with limiter:
        try:
            params = {
                'page': str(page), 'page_size': '25',
                'platform': 'DESKTOP', 'encrypted_input': encrypted_input,
                'recomGroupType': 'VSP', 'pageName': 'SRP', 'search_type': 'QS',
                'groupByConfigurations': 'true', 'origPageContext': {"searchScope":"","locationId":""},
                'lazy': 'true', 'isBottomNavFlow': 'false',
            }

            api_url = "https://www.99acres.com/api-aggregator/srp/search?" + urlencode(params)

            regenerated_token = regenerate_api_token(auth_token, api_url, "")
            if not regenerated_token:
                print(f"Failed to regenerate token for page {page}")
                return None

            headers = {
                'accept': '*/*', 'accept-language': 'en-US,en;q=0.9',
                'apitoken': regenerated_token, 'authorizationtoken': auth_token,
                'cache-control': 'no-cache', 'dnt': '1', 'pagename': 'SRP',
                'platform': 'desktop', 'pragma': 'no-cache',
                'priority': 'u=1, i', 'referer': ref_url,
                'sec-ch-ua': '"Chromium";v="133", "Not(A:Brand";v="99"',
                'sec-ch-ua-mobile': '?0', 'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty', 'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
            }

            # response = await session.get(api_url, headers=headers, cookies=cookies, proxies=proxies, impersonate="chrome110")
            response = await session.get(api_url, headers=headers, cookies=cookies, impersonate="chrome110")
            print(f"City: {city_id}, Page: {page}, Status: {response.status_code}")
            return response
        except Exception as e:
            print(f"An error occurred while fetching page {page} for city {city_id}: {e}")
            return None

async def get_initial_tokens(url, proxy):
    max_retries = 2
    for attempt in range(max_retries):
        authentication_token, encrypted_input, cookies = await get_authentication_token(url, proxy)
        if (authentication_token and cookies and encrypted_input):
            return authentication_token, encrypted_input, cookies
        print(f"Attempt {attempt + 1}")
        await asyncio.sleep(30)
    return None, None, None

async def process_city(session: AsyncSession, search_url: str, limiter: AsyncLimiter, results: dict[str, list]):
    """Processes all pages for a single city and stores data in the results dictionary."""
    city_name_match = re.search(r'/rent/([^/?]+)', search_url)
    if not city_name_match:
        print(f"Could not extract city name from {search_url}. Skipping.")
        return
    city_name = city_name_match.group(1)

    proxies: ProxySpec = ProxySpec(
        http = "",
        https = "",
    )

    # authentication_token, encrypted_input, cookies = await get_initial_tokens(search_url, proxies["http"])
    authentication_token, encrypted_input, cookies = await get_initial_tokens(search_url)
    if not (authentication_token and cookies and encrypted_input):
        print(f"Could not get initial tokens for {city_name}. Skipping.")
        return

    parts = urlparse(search_url)
    query_params = parse_qs(parts.query)
    city_id = query_params.get('city', [None])[0]
    if not city_id:
        print(f"Could not extract city_id from {search_url}. Skipping.")
        return

    results.setdefault(city_name, [])
    total_new_props = 0
    pgs_per_reqs = 5
    page = 1

    while True:
        tasks = [
            (page + i, fetch_page_data(session, page + i, city_id, authentication_token, search_url, encrypted_input, dict(cookies), limiter))
            for i in range(pgs_per_reqs)
        ]
        responses = await asyncio.gather(*[task[1] for task in tasks], return_exceptions=True)
        failed_pgs = []

        for (pg, _), response in zip(tasks, responses):
            if isinstance(response, Exception) or response is None or response.status_code != 200:
                print(f"Failed to fetch page {pg}: {response if isinstance(response, Exception) else 'None or non-200'}")
                failed_pgs.append(pg)

            else:
                try:
                    data = response.json()
                    try:
                        for property in data["properties"]:
                            results[city_name].append(property)

                        if total_new_props == 0:
                            total_new_props = data["count"]

                        print(f"total new properties found for {city_name}: {len(results[city_name])}")
                    except KeyError:
                        with open("response_data_error.json", "w", encoding="utf-8") as f:
                            json.dump(data, f, indent=4)
                except json.JSONDecodeError:
                    print(f"Failed to decode JSON from response for city '{city_name}'.")
                    should_break = True
                    reached_end = True
                    break
                except Exception as e:
                    print(f"Exception occured: {e}")

        if failed_pgs:
            print(f"Regenerating tokens for failed pages: {failed_pgs}")
            auth_token, encrypted_input, cookies = await get_initial_tokens(search_url)
            if not (authentication_token and cookies and encrypted_input):
                print(f"Could not get initial tokens for {city_name}. Skipping.")
                break
            retry_tasks = [
                fetch_page_data(session, pg, city_id, auth_token, search_url, encrypted_input, dict(cookies), limiter)
                for pg in failed_pgs
            ]
            retry_responses = await asyncio.gather(*retry_tasks, return_exceptions=True)
            for pg, response in zip(failed_pgs, retry_responses):
                if isinstance(response, Exception) or response is None or response.status_code != 200:
                    print(f"Retry failed for page {pg}: {response}")
                else:
                    try:
                        data = response.json()
                        results[city_name].extend(data.get("properties", []))
                    except json.JSONDecodeError as e:
                        print(f"JSON decode error on retry for page {pg}: {e}")

        if len(results[city_name]) >= total_new_props or page > 5:
            reached_end = True

        page += pgs_per_reqs


async def main():
    search_result_urls = [
        "https://www.99acres.com/search/property/rent/raipur?city=75&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/vadodara?city=96&preference=S&area_unit=1&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/bhubaneswar?city=162&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/mohali?city=172&keyword=mohali&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/noida?city=7&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/nashik?city=151&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/mysore?city=126&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/agra?city=197&keyword=agra&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/zirakpur-chandigarh?city=73&locality=2502&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/secunderabad?city=268&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/chandigarh?city=73&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/delhi?city=1075722&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/bhopal?city=140&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/meerut?city=207&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/varanasi?city=209&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/hyderabad?city=269&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/bareilly?city=200&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/thane?city=219&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/goa?city=233&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/patna?city=71&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/ahmedabad?city=45&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/kochi?city=131&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/guntur?city=54&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/trivandrum?city=138&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/aurangabad?city=147&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/allahabad?city=199&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/indore?city=142&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/rajkot?city=94&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/calicut?city=128&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/madurai?city=188&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/bangalore?city=20&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/visakhapatnam?city=62&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/pune?city=19&preference=S&area_unit=1&res_com=R",
        "https://www.99acres.com/search/property/rent/udaipur?city=181&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/bhiwadi?city=289&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/vijayawada?city=61&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/faridabad?city=10&keyword=faridabad&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/dehradun?city=211&keyword=dehradun&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/lucknow?city=205&keyword=lucknow&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/panchkula?city=256&keyword=panchkula&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/coimbato?city=185&keyword=coimbato&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/ranchi?city=117&keyword=ranchi&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/kota?city=180&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/ghaziabad?city=9&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/siliguri?city=283&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/guwahati?city=67&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/ludhiana?city=171&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/mangalore?city=125&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/chennai?city=32&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/jaipur?city=177&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/surat?city=95&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/trichy?city=192&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/sonipat?city=251&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/nagpur?city=150&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/gurgaon?city=8&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/mumbai?city=12&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/kanpur?city=204&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/thrissur?city=137&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/kolkata?city=25&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/kurnool?city=55&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/rajamahendravaram?city=1125767&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/gandhinagar?city=46&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/anand?city=84&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/bhimavaram?city=559&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/nellore?city=56&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/navi-mumbai?city=15&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/greater-noida?city=222&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/delhi-ncr?city=1&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/dharuhera?city=331&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/bhavnagar?city=87&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/karnal?city=101&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/ganjam?city=498&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
        "https://www.99acres.com/search/property/rent/berhampur?city=501&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N",
    ]

    all_scraped_data = {}
    limiter = AsyncLimiter(5, 2)

    async with AsyncSession() as session:
        for url in search_result_urls[0:1]:
            await process_city(session, url, limiter, all_scraped_data)
            print("-" * 60)

    print("\nScraping complete. Writing data to results.json...")
    try:
        with open("scrape_rental_results.json", "w", encoding="utf-8") as f:
            json.dump(all_scraped_data, f, indent=4)
        print("Data successfully saved to results.json")
    except Exception as e:
        print(f"Failed to write data to file: {e}")

    final_json_format = []
    for tag in all_scraped_data.keys():
        props = all_scraped_data[tag]
        if not isinstance(props, list):
            props = []
        for prop in props:
            if not isinstance(prop, dict):
                print(f"Warning: Skipping non-dictionary item found in data for tag '{tag}': {prop}")
                continue
            if not prop["BEDROOM_NUM"]:
                bed_num = 0
            prop_obj = {
                "description": prop.get("DESCRIPTION"),
                "status": prop.get("SECONDARY_TAGS", [None])[0],
                "latitude": prop.get("MAP_DETAILS", {}).get("LATITUDE"),
                "longitude": prop.get("MAP_DETAILS", {}).get("LONGITUDE"),
                "default_image": {
                    "url": prop.get("PHOTO_URL"),
                    "type": "IMAGE",
                    "source": None,
                    "status": "ENABLED"
                },
                "media": [
                    {"url": url, "type": "IMAGE", "source": None, "status": "ENABLED"}
                    for url in (prop.get("PROPERTY_IMAGES", []) or []) + (prop.get("THUMBNAIL_IMAGES", []) or [])
                ],
                "price": {
                    "currency": None,
                    "max_price": prop.get("MIN_PRICE"),
                    "min_price": prop.get("MAX_PRICE"),
                    "sft_price": prop.get("PRICE_SQFT"),
                    "floor_raise": None,
                    "effective_date": None
                },
                "phone_numbers": None,
                "project_type": prop.get("PROPERTY_TYPE"),
                "rera_details": {
                    "rera_number": "Not Available"
                },
                "is_gated_community": prop.get("GATED"),
                "loan_info": None,
                "amenities": prop.get("xid", {}).get("AMENITIES"),
                "area": prop.get("SUPERBUILTUP_SQFT"),
                "plot_venture_acres": None,
                "faqs": None,
                "cdata1": None,
                "cdata2": None,
                "created_date": {
                    "$date": {
                        "$numberLong": prop.get("POSTING_DATE__U")
                    }
                },
                "name": prop.get("PROP_NAME"),
                "possession_date": None,
                "country": "India",
                "state": None,
                "city_uid": None,
                "locality_uid": None,
                "address": prop.get("location", {}).get("ADDRESS"),
                "map_link": None,
                "no_blocks": None,
                "no_units": None,
                "no_floors": prop.get("TOTAL_FLOOR"),
                "builder_uid": None,
                "uid": None,
                "id": prop.get("PROP_ID"),
                "domain_id": None,
                "created_by": None,
                "group_buy": False,
                "open_house": None,
                "approval_status": "UPDATE_INPROGRESS",
                "neighbourhood_uids": None,
                "min_price": prop.get("MIN_PRICE"),
                "max_price": prop.get("MAX_PRICE"),
                "plan_urls": None,
                "brochure_urls": None,
                "coordinates": f"POINT({prop.get('MAP_DETAILS', {}).get('LATITUDE')} {prop.get('MAP_DETAILS', {}).get('LONGITUDE')})" if prop.get('MAP_DETAILS') else None,
                "rank": None,
                "advisors": None,
                "meta": None,
                "alias": "alpine-place-bangalore", # This seems like a hardcoded value, keeping as is
                "updated_date": {
                    "$date": {
                        "$numberLong": prop.get("UPDATE_DATE__U")
                    }
                },
                "updated_by": None,
                "is_featured": False,
                "cname_url": None,
                "has_open_house": False,
                "theme": None,
                "has_1bhk": prop.get("BEDROOM_NUM", 0) == '1',
                "has_2bhk": prop.get("BEDROOM_NUM", 0) == '2',
                "has_3bhk": prop.get("BEDROOM_NUM", 0) == '3',
                "has_4bhk": prop.get("BEDROOM_NUM", 0) == '4',
                "has_5bhk": prop.get("BEDROOM_NUM", 0) == '5',
                "has_5bhk_plus": bed_num > 5,
                "min_area1": 0,
                "max_area1": 0,
                "area_duplicate": None,
                "max_unit_area": None,
                "min_unit_area": None,
                "testimonials": None,
                "default_image_mobile": None,
                "three_d_house": None,
                "is_trending": None,
                "project_category": "RESIDENTIAL", # This seems like a hardcoded value, keeping as is
                "location": {
                    "type": "Point",
                    "coordinates": [
                        prop.get("MAP_DETAILS", {}).get("LATITUDE"),
                        prop.get("MAP_DETAILS", {}).get("LONGITUDE"),
                    ]
                }
            }
            final_json_format.append(prop_obj)

    try:
        with open("final_results_rental.json", "w", encoding="utf-8") as f:
            json.dump(final_json_format, f, indent=4)
        print("Data successfully saved to results.json")
    except Exception as e:
        print(f"Failed to write data to file: {e}")

if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
