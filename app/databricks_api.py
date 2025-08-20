import os
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}


class DatabricksAPIError(Exception):

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"Databricks API Error {status_code}: {message}")


def list_catalogs():
    resp = requests.get(
        f"{DATABRICKS_HOST}/api/2.1/unity-catalog/catalogs", headers=HEADERS
    )
    if not resp.ok:
        raise DatabricksAPIError(resp.status_code, resp.text)
    return resp.json() if resp.text.strip() else {"status": "success"}


def create_schema(schema_name: str, catalog_name: str):
    data = {"name": schema_name, "catalog_name": catalog_name}
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/unity-catalog/schemas", headers=HEADERS, json=data
    )
    if not resp.ok:
        raise DatabricksAPIError(resp.status_code, resp.text)
    return resp.json() if resp.text.strip() else {"status": "success"}


def create_volume(volume_name: str, schema_name: str, catalog_name: str):
    data = {
        "name": volume_name,
        "schema_name": schema_name,
        "catalog_name": catalog_name,
        "volume_type": "MANAGED",
    }
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/unity-catalog/volumes", headers=HEADERS, json=data
    )
    if not resp.ok:
        raise DatabricksAPIError(resp.status_code, resp.text)
    return resp.json() if resp.text.strip() else {"status": "success"}


def create_directory(
    directory_name: str, volume_name: str, schema_name: str, catalog_name: str
):
    directory_path = (
        f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{directory_name}"
    )
    endpoint = f"{DATABRICKS_HOST}/api/2.0/fs/directories{directory_path}"
    resp = requests.put(endpoint, headers=HEADERS)
    if not resp.ok:
        raise DatabricksAPIError(resp.status_code, resp.text)
    return resp.json() if resp.text.strip() else {"status": "success"}



def grant_permissions(object_type: str, full_name: str, access_payload: dict):

    endpoint = (
        f"{DATABRICKS_HOST}/api/2.1/unity-catalog/permissions/{object_type}/{full_name}"
    )

    resp = requests.patch(endpoint, headers=HEADERS, json=access_payload)
    if not resp.ok:
        raise DatabricksAPIError(resp.status_code, resp.text)
    return resp.json() if resp.text.strip() else {"status": "success"}
