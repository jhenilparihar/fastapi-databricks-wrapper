import time
import requests
from urllib.parse import urljoin
from app.core.config import DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_ACCOUNT_ID
from app.core.logging_config import get_logger

logger = get_logger("databricks_api")


HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}


class DatabricksAPIError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"Databricks API Error {status_code}: {message}")


def _make_request(
    method: str, endpoint: str, retries: int = 3, backoff: int = 2, **kwargs
):
    """
    - method: HTTP method ("GET", "POST", "PUT", "PATCH")
    - endpoint: relative endpoint (not full URL)
    - retries: number of retries on failure
    - backoff: seconds to wait (doubles each retry)
    """
    url = urljoin(DATABRICKS_HOST, endpoint)

    logger.info(f"Calling Databricks API: {method} {endpoint}")


    for attempt in range(retries):
        try:
            resp = requests.request(method, url, headers=HEADERS, timeout=30, **kwargs)

            if resp.ok:
                logger.info(f"Databricks API response: {resp.status_code} {resp.text}")
                return resp.json() if resp.text.strip() else {"status": "success"}

            if 400 <= resp.status_code < 500:  # Client errors (no retry)
                raise DatabricksAPIError(resp.status_code, resp.text)

            if 500 <= resp.status_code < 600:  # Server errors (retryable)
                raise requests.RequestException(
                    f"Server error {resp.status_code}: {resp.text}"
                )

            raise DatabricksAPIError(resp.status_code, resp.text)

        except (requests.RequestException, requests.Timeout) as e:
            if attempt < retries - 1:
                wait = backoff * (2**attempt)
                print(
                    f"[Retry {attempt+1}] Request failed: {e}, retrying in {wait}s..."
                )
                time.sleep(wait)
                continue
            status = getattr(getattr(e, "response", None), "status_code", 500)
            raise DatabricksAPIError(status, str(e))

    raise DatabricksAPIError(-1, f"Max retries exceeded for endpoint: {endpoint}")


def list_catalogs():
    return _make_request("GET", "/api/2.1/unity-catalog/catalogs")


def create_schema(schema_name: str, catalog_name: str):
    data = {"name": schema_name, "catalog_name": catalog_name}
    return _make_request("POST", "/api/2.1/unity-catalog/schemas", json=data)


def create_volume(volume_name: str, schema_name: str, catalog_name: str):
    data = {
        "name": volume_name,
        "schema_name": schema_name,
        "catalog_name": catalog_name,
        "volume_type": "MANAGED",
    }
    return _make_request("POST", "/api/2.1/unity-catalog/volumes", json=data)


def create_directory(
    directory_name: str, volume_name: str, schema_name: str, catalog_name: str
):
    directory_path = (
        f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{directory_name}"
    )
    endpoint = f"/api/2.0/fs/directories{directory_path}"
    return _make_request("PUT", endpoint)


def grant_permissions(object_type: str, full_name: str, access_payload: dict):
    endpoint = f"/api/2.1/unity-catalog/permissions/{object_type}/{full_name}"
    return _make_request("PATCH", endpoint, json=access_payload)


def list_groups(filter_name: str = None):
    """
    List all groups (optionally filter by group name).
    """
    endpoint = f"/api/2.0/accounts/{DATABRICKS_ACCOUNT_ID}/scim/v2/Groups"
    if filter_name:
        endpoint += f"?filter=displayName eq '{filter_name}'"
    return _make_request("GET", endpoint)


def create_group(group_name: str):
    """
    Create a new group in Databricks.
    """
    data = {"displayName": group_name}
    return _make_request(
        "POST", f"/api/2.0/accounts/{DATABRICKS_ACCOUNT_ID}/scim/v2/Groups", json=data
    )


def ensure_group_exists(group_name: str, retries: int = 3, backoff: int = 2):
    resp = list_groups(filter_name=group_name)

    if resp.get("totalResults", 0) > 0:
        return resp["Resources"][0]

    create_group(group_name)

    # retry until Unity Catalog can see it
    for attempt in range(retries):
        resp = list_groups(filter_name=group_name)
        if resp.get("totalResults", 0) > 0:
            return resp["Resources"][0]
        wait = backoff * (2**attempt)
        print(
            f"[Retry {attempt+1}] Group {group_name} not visible yet, retrying in {wait}s..."
        )
        time.sleep(wait)

    raise DatabricksAPIError(
        -1, f"Group {group_name} created but not visible after retries"
    )
