import re
import time
import json
import asyncio
import base64
import base64
import hashlib
import hmac # Added for HMAC signing
import binascii # Added for base64 decoding errors
from typing import Any
import hashlib
import hmac # Added for HMAC signing
import binascii # Added for base64 decoding errors
from typing import Any
from get_cookies import extract_cookies_99acres

async def get_authentication_token(url: str, proxies: str) -> tuple[str, str, list[Any]]:
    """
    Fetches the authentication token from the 99acres.com homepage.
    """
    try:

        # html_content, cookies = await asyncio.to_thread(extract_cookies_99acres, url, proxies)
        html_content, cookies = await asyncio.to_thread(extract_cookies_99acres, url, proxies)

        # with open("html_content.html", "w", encoding="utf-8") as file:
        #     file.write(html_content)

        api_token = ""
        match = re.search(r'__apiToken" value="([^"]+)"', html_content)
        if match:
            api_token = match.group(1)
            print(f"Extracted __apiToken: {api_token}")
        else:
            print("API Token not found.")
            return "", "", []

        encrypted_input = ""
        match = re.search(r'"encrypted_input":"([^"]+)"', html_content)
        if match:
            encrypted_input = match.group(1)
        else:
            print("encrypted_input not found")
            return "", "", []

        return api_token, encrypted_input, cookies

    except Exception as e:
        print(f"An unexpected error occurred in get_authentication_token: {e}")
        return "", "", []

    return "", "", []

def decode_base64_string(base64_str: str) -> str | None:
    """
    Decodes a URL-safe base64 string.
    """
    clean_base64_str: str = base64_str.strip()
    clean_base64_str = clean_base64_str.replace('-', '+').replace('_', '/')
    missing_padding: int = len(clean_base64_str) % 4
    if missing_padding:
        clean_base64_str += '=' * (4 - missing_padding)

    try:
        decoded_bytes: bytes = base64.b64decode(clean_base64_str)
        l_result: str = decoded_bytes.decode('ascii')
        return l_result
    except (TypeError, binascii.Error) as e:
        print(f"Error decoding base64 string: {e}. Input: {base64_str}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during decoding: {e}")
        return None

def encode_urlsafe_base64(data_string: str) -> str:
    """URL-safe base64 encoding."""
    return base64.b64encode(data_string.encode('utf-8')).decode('utf-8').replace('+', '-').replace('/', '_').rstrip('=')

def calculate_md5_hash(text: str) -> str | None:
    """MD5 hashing."""
    try:
        return hashlib.md5(text.encode('utf-8')).hexdigest()
    except Exception as e:
        print(f"Error calculating SHA1 hash: {e}")
        return None

def generate_auth_token(
    salt1: str,
    salt2: str,
    base64_secret: str,
    query_string: str,
    request_body: str
) -> str | None:
    """Generates the JWT with descriptive variable names."""
    query_hash: str | None = calculate_md5_hash(salt1 + query_string + salt2)
    body_hash: str | None = calculate_md5_hash(salt1 + request_body + salt2)

    if query_hash is None or (request_body and body_hash is None):
        print("Error: Could not generate necessary hashes for token.")
        return None

    try:
        # Decode the secret key for the signature
        hmac_key: bytes = base64.b64decode(base64_secret)
    except (TypeError, binascii.Error) as e:
        print(f"Error decoding base64 secret for HMAC key: {e}")
        return None

    # Assume webdriver is false for server-side generation
    is_webdriver: bool = False
    webdriver_hash: str | None = calculate_md5_hash(salt1 + str(is_webdriver).lower() + salt2)
    if webdriver_hash is None:
        print("Error: Could not generate webdriver hash.")
        return None

    issued_time = round(time.time(), 3)

    payload: dict[str, Any] = {
        "iat": issued_time,
        "exp": issued_time + 120,
        "hq": query_hash, # Query Hash
        "wb": webdriver_hash # Webdriver Hash
    }
    if request_body:
        payload["hb"] = body_hash # Body Hash

    # Create the token parts (header and payload)
    header_payload: dict[str, str] = {"typ": "JWT", "alg": "HS256"}
    encoded_header: str = encode_urlsafe_base64(json.dumps(header_payload, separators=(',', ':')))
    encoded_payload: str = encode_urlsafe_base64(json.dumps(payload, separators=(',', ':')))

    unsigned_token: str = f"{encoded_header}.{encoded_payload}"

    try:
        # Create the HMAC SHA256 signature
        signature: bytes = hmac.new(hmac_key, unsigned_token.encode('utf-8'), hashlib.sha256).digest()
        encoded_signature: str = base64.b64encode(signature).decode('utf-8').replace('+', '-').replace('/', '_').rstrip('=')
    except Exception as e:
        print(f"Error generating HMAC signature: {e}")
        return None

    return f"{unsigned_token}.{encoded_signature}"

def regenerate_api_token(auth_token: str, url: str, options_body: str) -> str | None:
    """Decodes an existing token and generates a new one."""
    parts: list[str] = auth_token.split('.')
    if len(parts) == 3:
        try:
            # Decode the payload from the original token
            payload_b64: str = parts[1]
            padded_payload: str = payload_b64 + '=' * (-len(payload_b64) % 4)
            decoded_payload: dict[str, Any] = json.loads(base64.b64decode(padded_payload.replace('-', '+').replace('_', '/')).decode('utf-8'))

            # Extract salts and secret from the decoded payload
            salt1: str | None = decoded_payload.get("s1")
            salt2: str | None = decoded_payload.get("s2")
            base64_secret: str | None = decoded_payload.get("s3")

            if not (isinstance(salt1, str) and
                    isinstance(salt2, str) and
                    isinstance(base64_secret, str)):
                print("Error: Missing or invalid salt/secret in decoded token payload.")
                return None

            # Extract query string from the URL
            query_string: str = url.split('?', 1)[1] if '?' in url else ''

            s1: str = salt1
            s2: str = salt2
            s3: str = base64_secret

            # Generate the new token
            new_token: str|None = generate_auth_token(s1, s2, s3, query_string, options_body)
            return new_token
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from token payload: {e}")
            return None
        except (TypeError, binascii.Error) as e:
            print(f"Error in base64 decoding during token regeneration: {e}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred during token regeneration: {e}")
            return None
    else:
        print(f"Invalid authentication token format. Expected 3 parts, got {len(parts)}.")
        return None
