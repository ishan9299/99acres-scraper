import ast
import mycdp
import sys
import time
import json
import base64
from seleniumbase import SB

requests = []

async def receiveResponseBody(page, requests_list):
    responses = []
    for request in requests_list:
        try:
            if "https://www.99acres.com/search/property/" or "https://www.99acres.com/new-projects-in" in request[0]:
                # print(f"Request is: {request[0]}")
                # print(f"Fetching response body")

                res_body_data = await page.send(mycdp.network.get_response_body(request[1]))

                if res_body_data is None:
                    # print(f"No response body data for {request[0]}")
                    continue

                res_content = res_body_data[0]

                responses.append(res_content)
        except Exception as e:
            print(f"Error getting response body for {request[0]}: {e}")
    return responses

async def receiveCookies(page, requests_list):
    found_target_url = False
    cookies_result = []

    print(f"Attempting to fetch cookies")

    for request_url, _ in requests_list:
        if "https://www.99acres.com/api-aggregator/auth/doStaticPageLogin" in request_url and not found_target_url:
            try:
                cookies_data = await page.send(mycdp.network.get_cookies(urls=[request_url]))

                if cookies_data:
                    print(f"Cookies found for {request_url}:")
                    for cookie in cookies_data:
                        # print(f"  Name: {cookie.name}, Value: {cookie.value}, Domain: {cookie.domain}, Path: {cookie.path}")
                        cookies_result.append(cookie)
                    found_target_url = True
                else:
                    print(f"No cookies found for {request_url}")
            except Exception as e:
                print(f"Error fetching cookies for {request_url}: {e}")

            if found_target_url:
                break

    if not found_target_url:
        print(f"No cookies found.")

    return cookies_result

def extract_cookies_99acres(url, proxy):
    # with SB(uc=True, test=True, locale="en", headless2=False, proxy=proxy) as sb:
    with SB(uc=True, test=True, locale="en", headless2=False) as sb:
        sb.activate_cdp_mode("about:blank")
        page = sb.cdp.page

        async def handler(evt):
            requests.append([evt.response.url, evt.request_id])

        loop = sb.cdp.get_event_loop()

        loop.run_until_complete(page.send(mycdp.network.enable()))
        page.add_handler(mycdp.network.ResponseReceived, handler)
        target_url = url
        print(f"Opening URL: {target_url}")
        sb.cdp.open(target_url)
        time.sleep(30)

        print(f"Scrolling down to trigger more XHRs...")
        for i in range(5):
            sb.cdp.scroll_down(300)
            time.sleep(5)

        time.sleep(2)

        print(f"Processing captured XHR responses (fetching bodies)...")
        responses = loop.run_until_complete(receiveResponseBody(page, requests))
        print(f"Total {len(responses)} XHR response bodies processed.")
        if len(responses) == 0:
            responses.append(sb.cdp.get_page_source())

        print(f"Fetching cookies for the first matching request URL...")
        cookies = loop.run_until_complete(receiveCookies(page, requests)) 
        cookie_dict = {cookie.name: cookie.value for cookie in cookies}
        print("Cookies formatted for curl_cffi:")

        print(f"Script finished.")
        return responses[0], cookie_dict

