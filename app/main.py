from fastapi import FastAPI
from databricks_api import list_catalog

app = FastAPI()


@app.get("/catalog")
def get_catalog():
    return list_catalog()
