import re
import math
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
from urllib.parse import urlencode, urljoin

def get_json_from_html(html_str: str):
    tree = html.fromstring(html_str)
    script_xpath: str = '//script[contains(text(), "window.__initialData__=")]'
    script_elements = tree.xpath(script_xpath)
    if not script_elements:
        print("No script tag containing 'window.__initialData__ =' found.")

    script_content = script_elements[0].text_content()
    start_marker = "window.__initialData__="
    start_index = script_content.find(start_marker)
    if start_index == -1:
        print("start marker 'window.__initData__=' not found")

    json_start_index = start_index + len(start_marker)
    json_string_raw = script_content[json_start_index:].strip()
    stack: list[str] = []

    json_str_raw_len: int = len(json_string_raw)
    json_end: int = 0
    for i in range(0, json_str_raw_len):
        ch = json_string_raw[i]
        if ch == "{":
            stack.append(ch)
        elif ch == "}":
            _ = stack.pop()
            if not stack:
                json_end = i + 1
    try:
        json_string_raw = json_string_raw[:json_end].strip()
    except Exception as e:
        print(f"Exception occured when stripping json: {e}")

    data: dict[str, Any] = {}

    try:
        data = json.loads(json_string_raw)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

    return data

async def process_city(session: AsyncSession, search_url: list[dict], limiter: AsyncLimiter, results: list[dict[str, Any]]):
    """Processes all pages for a single city and stores data in the results dictionary."""
    proxies: ProxySpec = ProxySpec(
        http = "",
        https = "",
    )

    def append_builder_data(pageData, builder_data):
        components = pageData.get("components", [])
        if components:
            data = components[0].get("data", {})
            for card in data.get("cards", []):
                card_data = card.get("data", {})
                if card_data:
                    card_data.pop('subCards', None)
                    builder_data.append(card_data)

    builder_data = []
    print(f"getting builder data for: {search_url['url']}")
    async with limiter:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'dnt': '1',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Chromium";v="133", "Not(A:Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
        }
        # response = await session.get(search_url["url"], headers=headers, proxies=proxies, impersonate="chrome")
        response = await session.get(search_url["url"], headers=headers, impersonate="chrome")

    response.raise_for_status()
    first_pg_data = get_json_from_html(response.content)
    builderSrp = first_pg_data.get("builderSrp", {})
    pageData = builderSrp.get("pageData", {})

    append_builder_data(pageData, builder_data)
    print(f"builder data is: {builder_data}")

    basicDetails = pageData.get("basicDetails", {})
    print(f"basic details is: {basicDetails}")
    total_builders = basicDetails.get("resultCount", 0)
    end_page = math.ceil(total_builders / 10)

    ref_url: str = search_url["url"]

    tasks = []
    for pg in range(2, end_page):
        base_url: str = f"{search_url['url']}-page-{pg}"
        params: dict[str, Any] = {
            "city": search_url["city"],
            "resCom": "",
            "page": pg,
            "sortby": "popularity",
        }

        query_string = urlencode(params)
        complete_url = urljoin(base_url, f"?{query_string}")

        async with limiter:
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'no-cache',
                'dnt': '1',
                'pragma': 'no-cache',
                'priority': 'u=0, i',
                'referer': ref_url,
                'sec-ch-ua': '"Chromium";v="133", "Not(A:Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
            }

            print(f"getting builder data from: {complete_url}")
            # response = await session.get(complete_url, headers=headers, proxies=proxies, impersonate="chrome")
            response = await session.get(complete_url, headers=headers, impersonate="chrome")
            tasks.append(response)

        ref_url = complete_url
    responses = await asyncio.gather(*tasks)

    for response in responses:
        first_pg_data = get_json_from_html(response.content)
        print(first_pg_data)
        builderSrp = first_pg_data.get("builderSrp", {})
        pageData = builderSrp.get("pageData", {})

        append_builder_data(pageData, builder_data)

    # i have ids here I will iterate over these and get the projects
    for data in builder_data:
        ref_url: str = f"https://www.99acres.com/new-projects-in-{search_url['city']}-ffid?builderid={data['builderId']}"

        retry = 0

        while retry < 3:
            # auth_token, encrypted_input, cookies = await get_authentication_token(ref_url, proxies["http"])
            auth_token, encrypted_input, cookies = await get_authentication_token(ref_url)
            if not (auth_token and cookies and encrypted_input):
                print(f"Could not get initial tokens for {ref_url}. Retrying.")
                retry += 1
            else:
                break

        if not (auth_token and cookies and encrypted_input):
            print(f"Could not get initial tokens for {ref_url}. Skipping.")
            continue

        headers = {
          'accept': '*/*',
          'accept-language': 'en-US,en;q=0.9',
          'cache-control': 'no-cache',
          'dnt': '1',
          'pagename': 'NPSRP',
          'platform': 'desktop',
          'pragma': 'no-cache',
          'priority': 'u=1, i',
          'referer': ref_url,
          'sec-ch-ua': '"Chromium";v="133", "Not(A:Brand";v="99"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"Windows"',
          'sec-fetch-dest': 'empty',
          'sec-fetch-mode': 'cors',
          'sec-fetch-site': 'same-origin',
          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
        }

        props_remaining = data["projectCount"]["total"]["value"]

        pg = 1
        properties = []

        while props_remaining > 0:

            print("=" * 60)
            api_url: str = f"https://www.99acres.com/api-aggregator/project/search?builderid={data['builderId']}&builder={data['builderId']}&res_com=R&sortby=sab_default&cityID={search_url['id']}&page={pg}&noxid=Y&isAjax=true&city={search_url['id']}&platform=DESKTOP&lazy=true&recomGroupType=VSP&builderid={data['builderId']}&pageName=NPSRP&groupByConfigurations=true&lazy=true"
            # api_url = "https://www.99acres.com/api-aggregator/project/search?" + urlencode(params)
            print(f"api_url is: {api_url}")
            regenerated_token = regenerate_api_token(auth_token, api_url, "")
            if not regenerated_token:
                print(f"Failed to regenerate token for page {page}")
                return None

            headers['apitoken'] = regenerated_token
            headers['authorizationtoken'] = auth_token

            # response new Projects and secondary Projects
            try:
                # response = await session.get(api_url, headers=headers, cookies=cookies, proxies=proxies, impersonate="chrome")
                response = await session.get(api_url, headers=headers, cookies=cookies, impersonate="chrome")
                propertyData = response.json()
                num_new_projs = len(propertyData["newProjects"])
                num_sec_new_projs = len(propertyData["secondaryNewProjects"])
                props_remaining -= num_new_projs
                props_remaining -= num_sec_new_projs
                properties += [*propertyData["newProjects"], *propertyData["secondaryNewProjects"]]
                print(f"num of new projects: {num_new_projs}")
                print(f"num of secondary new projects: {num_sec_new_projs}")
                print(f"page: {pg}, properties_remaining: {props_remaining}")
                pg += 1
                if num_new_projs == 0 and num_sec_new_projs == 0:
                    props_remaining = 0
            except curl_cffi.requests.exceptions.HTTPError as e:
                print(f"HTTP error occurred: {e}")
                continue
            except KeyError:
                props_remaining = 0
            except json.JSONDecodeError:
                print(f"Response Content: {response.content[:200]}") # Log the start of the response
            except Exception as e:
                print(f"An error with exception: {e} has occured")
            print("=" * 60)

        data["scraped_properties"] = properties
        results.append(data)

    with open("scraped_data_builder.json", "w", encoding="utf-8") as file:
        json.dump(results, file, indent=2)


async def main():
    search_result_urls = [
        {"url": "https://www.99acres.com/builders-in-raipur-bffid", "city": "raipur", "id": 75},
        {"url": "https://www.99acres.com/builders-in-vadodara-bffid", "city": "vadodara", "id": 96},
        {"url": "https://www.99acres.com/builders-in-bhubaneswar-bffid", "city": "bhubaneswar", "id": 162},
        {"url": "https://www.99acres.com/builders-in-mohali-bffid", "city": "mohali", "id": 172},
        {"url": "https://www.99acres.com/builders-in-noida-bffid", "city": "noida", "id": 7},
        {"url": "https://www.99acres.com/builders-in-nashik-bffid", "city": "nashik", "id": 151},
        {"url": "https://www.99acres.com/builders-in-mysore-bffid", "city": "mysore", "id": 126},
        {"url": "https://www.99acres.com/builders-in-agra-bffid", "city": "agra", "id": 197},
        {"url": "https://www.99acres.com/builders-in-zirakpur-chandigarh-bffid", "city": "zirakpur-chandigarh", "id": 73},
        {"url": "https://www.99acres.com/builders-in-secunderabad-bffid", "city": "secunderabad", "id": 268},
        {"url": "https://www.99acres.com/builders-in-chandigarh-bffid", "city": "chandigarh", "id": 73},
        {"url": "https://www.99acres.com/builders-in-delhi-bffid", "city": "delhi", "id": 1075722},
        {"url": "https://www.99acres.com/builders-in-bhopal-bffid", "city": "bhopal", "id": 140},
        {"url": "https://www.99acres.com/builders-in-meerut-bffid", "city": "meerut", "id": 207},
        {"url": "https://www.99acres.com/builders-in-varanasi-bffid", "city": "varanasi", "id": 209},
        {"url": "https://www.99acres.com/builders-in-hyderabad-bffid", "city": "hyderabad", "id": 269},
        {"url": "https://www.99acres.com/builders-in-bareilly-bffid", "city": "bareilly", "id": 200},
        {"url": "https://www.99acres.com/builders-in-thane-bffid", "city": "thane", "id": 219},
        {"url": "https://www.99acres.com/builders-in-goa-bffid", "city": "goa", "id": 233},
        {"url": "https://www.99acres.com/builders-in-patna-bffid", "city": "patna", "id": 71},
        {"url": "https://www.99acres.com/builders-in-ahmedabad-bffid", "city": "ahmedabad", "id": 45},
        {"url": "https://www.99acres.com/builders-in-kochi-bffid", "city": "kochi", "id": 131},
        {"url": "https://www.99acres.com/builders-in-guntur-bffid", "city": "guntur", "id": 54},
        {"url": "https://www.99acres.com/builders-in-trivandrum-bffid", "city": "trivandrum", "id": 138},
        {"url": "https://www.99acres.com/builders-in-aurangabad-bffid", "city": "aurangabad", "id": 147},
        {"url": "https://www.99acres.com/builders-in-allahabad-bffid", "city": "allahabad", "id": 199},
        {"url": "https://www.99acres.com/builders-in-indore-bffid", "city": "indore", "id": 142},
        {"url": "https://www.99acres.com/builders-in-rajkot-bffid", "city": "rajkot", "id": 94},
        {"url": "https://www.99acres.com/builders-in-calicut-bffid", "city": "calicut", "id": 128},
        {"url": "https://www.99acres.com/builders-in-madurai-bffid", "city": "madurai", "id": 188},
        {"url": "https://www.99acres.com/builders-in-bangalore-bffid", "city": "bangalore", "id": 20},
        {"url": "https://www.99acres.com/builders-in-visakhapatnam-bffid", "city": "visakhapatnam", "id": 62},
        {"url": "https://www.99acres.com/builders-in-pune-bffid", "city": "pune", "id": 19},
        {"url": "https://www.99acres.com/builders-in-udaipur-bffid", "city": "udaipur", "id": 181},
        {"url": "https://www.99acres.com/builders-in-bhiwadi-bffid", "city": "bhiwadi", "id": 289},
        {"url": "https://www.99acres.com/builders-in-vijayawada-bffid", "city": "vijayawada", "id": 61},
        {"url": "https://www.99acres.com/builders-in-faridabad-bffid", "city": "faridabad", "id": 10},
        {"url": "https://www.99acres.com/builders-in-dehradun-bffid", "city": "dehradun", "id": 211},
        {"url": "https://www.99acres.com/builders-in-lucknow-bffid", "city": "lucknow", "id": 205},
        {"url": "https://www.99acres.com/builders-in-panchkula-bffid", "city": "panchkula", "id": 256},
        {"url": "https://www.99acres.com/builders-in-coimbato-bffid", "city": "coimbato", "id": 185},
        {"url": "https://www.99acres.com/builders-in-ranchi-bffid", "city": "ranchi", "id": 117},
        {"url": "https://www.99acres.com/builders-in-kota-bffid", "city": "kota", "id": 180},
        {"url": "https://www.99acres.com/builders-in-ghaziabad-bffid", "city": "ghaziabad", "id": 9},
        {"url": "https://www.99acres.com/builders-in-siliguri-bffid", "city": "siliguri", "id": 283},
        {"url": "https://www.99acres.com/builders-in-guwahati-bffid", "city": "guwahati", "id": 67},
        {"url": "https://www.99acres.com/builders-in-ludhiana-bffid", "city": "ludhiana", "id": 171},
        {"url": "https://www.99acres.com/builders-in-mangalore-bffid", "city": "mangalore", "id": 125},
        {"url": "https://www.99acres.com/builders-in-chennai-bffid", "city": "chennai", "id": 32},
        {"url": "https://www.99acres.com/builders-in-jaipur-bffid", "city": "jaipur", "id": 177},
        {"url": "https://www.99acres.com/builders-in-surat-bffid", "city": "surat", "id": 95},
        {"url": "https://www.99acres.com/builders-in-trichy-bffid", "city": "trichy", "id": 192},
        {"url": "https://www.99acres.com/builders-in-sonipat-bffid", "city": "sonipat", "id": 251},
        {"url": "https://www.99acres.com/builders-in-nagpur-bffid", "city": "nagpur", "id": 150},
        {"url": "https://www.99acres.com/builders-in-gurgaon-bffid", "city": "gurgaon", "id": 8},
        {"url": "https://www.99acres.com/builders-in-mumbai-bffid", "city": "mumbai", "id": 12},
        {"url": "https://www.99acres.com/builders-in-kanpur-bffid", "city": "kanpur", "id": 204},
        {"url": "https://www.99acres.com/builders-in-thrissur-bffid", "city": "thrissur", "id": 137},
        {"url": "https://www.99acres.com/builders-in-kolkata-bffid", "city": "kolkata", "id": 25},
        {"url": "https://www.99acres.com/builders-in-kurnool-bffid", "city": "kurnool", "id": 55},
        {"url": "https://www.99acres.com/builders-in-rajamahendravaram-bffid", "city": "rajamahendravaram", "id": 1125767},
        {"url": "https://www.99acres.com/builders-in-gandhinagar-bffid", "city": "gandhinagar", "id": 46},
        {"url": "https://www.99acres.com/builders-in-anand-bffid", "city": "anand", "id": 84},
        {"url": "https://www.99acres.com/builders-in-bhimavaram-bffid", "city": "bhimavaram", "id": 559},
        {"url": "https://www.99acres.com/builders-in-nellore-bffid", "city": "nellore", "id": 56},
        {"url": "https://www.99acres.com/builders-in-navi-mumbai-bffid", "city": "navi-mumbai", "id": 15},
        {"url": "https://www.99acres.com/builders-in-greater-noida-bffid", "city": "greater-noida", "id": 222},
        {"url": "https://www.99acres.com/builders-in-delhi-ncr-bffid", "city": "delhi-ncr", "id": 1},
        {"url": "https://www.99acres.com/builders-in-dharuhera-bffid", "city": "dharuhera", "id": 331},
        {"url": "https://www.99acres.com/builders-in-bhavnagar-bffid", "city": "bhavnagar", "id": 87},
        {"url": "https://www.99acres.com/builders-in-karnal-bffid", "city": "karnal", "id": 101},
        {"url": "https://www.99acres.com/builders-in-ganjam-bffid", "city": "ganjam", "id": 498},
        {"url": "https://www.99acres.com/builders-in-berhampur-bffid", "city": "berhampur", "id": 501},
    ]

    all_scraped_data = []
    limiter = AsyncLimiter(5, 2)

    async with AsyncSession() as session:
        for url in search_result_urls[0:1]:
            await process_city(session, url, limiter, all_scraped_data)
            print("-" * 60)

    with open("builder_data_results.json", "w", encoding="utf-8") as file:
        json.dump(all_scraped_data, file, indent=2)

    final_json_format = []
    for tag in all_scraped_data.keys():
        props = all_scraped_data[tag]
        if not isinstance(props, list):
            props = []
        for prop in props:
            if not isinstance(prop, dict):
                print(f"Warning: Skipping non-dictionary item found in data for tag '{tag}': {prop}")
                continue
            prop_obj = {
                    "description": prop["description"]["text"],
                    "phone_numbers": [],
                    "emails": [],
                    "awards": [],
                    "achievements": [],
                    "faq": [],
                    "status": "ACTIVE",
                    "verification_status": {},
                    "builder_grade": "",
                    "builder_status": "",
                    "business_potential": "",
                    "social": None,
                    "media": None,
                    "certifications": [],
                    "offices": [
                        {
                            "source": None,
                            "address": None,
                            "city_uid": None,
                            "headoffice": False,
                            "location_uid": None
                            }
                        ],
                    "vision": None,
                    "mission": None,
                    "third_party_urls": [
                        {
                            "url": "",
                            "name": ""
                        }
                    ],
                    "created_date": {
                        "$date": {
                            "$numberLong": ""
                        }
                    },
                    "name": prop["name"],
                    "primary_email": "",
                    "website": "",
                    "total_projects": prop["projectCount"]["total"]["value"],
                    "completed_projects": prop["projectCount"]["tuples"][0]["value"],
                    "ongoing_projects": prop["projectCount"]["tuples"][1]["value"],
                    "upcoming_projects": prop["projectCount"]["tuples"][1]["value"],
                    "total_experience": None,
                    "logo": prop["coverImage"]["url"],
                    "id": prop["builderId"],
                    "domain_id": "",
                    "created_by": "",
                    "uid": "",
                    "testimonials": None,
                    "cdata1": {
                            "news": [
                                {
                                    "url": "",
                                    "date": None,
                                    "title": "",
                                    "description": ""
                                    }
                                ],
                            "blogs": [
                                {
                                    "url": "",
                                    "date": None,
                                    "title": "",
                                    "sub_title": "",
                                    "description": ""
                                    }
                                ],
                            "press": [
                                {
                                    "url": "",
                                    "date": None,
                                    "title": "",
                                    "description": ""
                                    }
                                ],
                            "map_url": {
                                "map_link": ""
                                }
                            },
                    "chairman_info": {
                            "title": None,
                            "message": None,
                            "image_url": None
                            },
                    "approval_status": "UPDATE_INPROGRESS",
                    "leadership": [],
                    "features": None,
                    "advisors": None,
                    "meta": None,
                    "alias": prop["name"],
                    "updated_date": {
                            "$date": {
                                "$numberLong": ""
                                }
                            },
                    "updated_by": "",
                    "cname_url": None,
                    "theme": None,
                    "is_synced": False,
                    "telephony": None,
                    "sip_phone_number": None,
                    "sales_emails": [],
                    "sales_sms_numbers": [],
                    "sales_whatsapp_numbers": [],
                    "is_client": None
            }
            final_json_format.append(prop_obj)

    try:
        with open("final_results_builder.json", "w", encoding="utf-8") as f:
            json.dump(final_json_format, f, indent=4)
        print("Data successfully saved to results.json")
    except Exception as e:
        print(f"Failed to write data to file: {e}")

if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
