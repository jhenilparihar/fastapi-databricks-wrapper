from fastapi import FastAPI, HTTPException
from pydantic_model import Payload
from service import process_payload
from databricks_api import DatabricksAPIError

app = FastAPI()


@app.post("/process")
def process_request(payload: Payload):
    try:
        return process_payload(payload)
    except DatabricksAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
