import os
from dotenv import load_dotenv
import requests

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")


def list_catalog():
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    endpoint = "/api/2.1/unity-catalog/catalogs"
    resp = requests.get(f"{DATABRICKS_HOST}{endpoint}", headers=headers)

    return resp.json()
