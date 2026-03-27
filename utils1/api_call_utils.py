
# ----------------------------
# SETUP ROBUST SESSION FOR PM.COM API CALLS v1
# ----------------------------
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import ConnectionError, HTTPError
from urllib3.exceptions import NameResolutionError

session = requests.Session()
retry_strategy = Retry(
    total=10,  # total retries for all errors
    connect=5,  # retries specifically for connection errors (DNS)
    read=3,  # retries for read errors
    backoff_factor=1,  # exponential backoff 1s, 2s, 4s...
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "PUT", "POST", "DELETE"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)


def robust_get(url, headers, logger, timeout=30):
    try:
        resp = session.get(url, headers=headers, timeout=timeout)
        if resp.status_code == 404:
            try:
                payload = resp.json()
            except ValueError:
                payload = {}
            if payload.get("statusCode") == "NotFound":
                logger.info(f"PM.com field has no value (treating as blank): {url}")
                return {}  # Acts like blank / missing data
            resp.raise_for_status()
        resp.raise_for_status()
        return resp.json()
    except ConnectionError as e:
        if isinstance(e.__cause__, NameResolutionError):
            logger.warning(f"Temporary DNS issue for {url}, will retry: {e}")
            raise
        else:
            raise
    except HTTPError as e:
        logger.error(f"HTTP error {e.response.status_code} for {url}")
        raise


def robust_put(url, headers, payload, logger, timeout=30):
    try:
        resp = session.put(url, headers=headers, json=payload, timeout=timeout)
        resp.raise_for_status()
        return resp
    except ConnectionError as e:
        if isinstance(e.__cause__, NameResolutionError):
            logger.warning(f"Temporary DNS issue for {url}, will retry: {e}")
            raise
        else:
            raise
    except HTTPError as e:
        logger.error(f"HTTP error {e.response.status_code} for {url}")
        raise


def robust_post(url, headers, payload, logger, timeout=30):
    try:
        resp = session.post(url, headers=headers, json=payload, timeout=timeout)
        print()
        resp.raise_for_status()

        data = resp.json()

        # Handle PM.com "soft errors" in response body
        if isinstance(data, dict) and data.get("hasError"):
            logger.error(f"PM.com API logical error: {data.get('error')}")
            raise Exception(f"PM.com logical error: {data.get('error')}")

        return resp

    except ConnectionError as e:
        if isinstance(e.__cause__, NameResolutionError):
            logger.warning(f"Temporary DNS issue for {url}, will retry: {e}")
            raise
        else:
            raise

    except HTTPError as e:
        try:
            error_body = e.response.json()
        except:
            error_body = e.response.text

        logger.error(f"HTTP error {e.response.status_code} for {url}: {error_body}")
        raise


def robust_delete(url, headers, logger, timeout=30):
    try:
        resp = session.delete(url, headers=headers, timeout=timeout)

        # Handle already-deleted cases gracefully
        if resp.status_code == 404:
            logger.info(f"Already deleted or not found: {url}")
            return None

        resp.raise_for_status()
        # print(resp.status_code)
        # print(resp.json())
        return resp

    except ConnectionError as e:
        if isinstance(e.__cause__, NameResolutionError):
            logger.warning(f"Temporary DNS issue for {url}, will retry: {e}")
            raise
        else:
            raise

    except HTTPError as e:
        logger.error(f"HTTP error {e.response.status_code} for {url}")
        raise