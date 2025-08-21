from tokenize import group
from fastapi import FastAPI, HTTPException
from app.schemas.study_payload import StudyPayload
from app.services.study_resources import process_payload
from app.databricks_api import DatabricksAPIError
from app.core.config import DATABRICKS_HOST, DATABRICKS_TOKEN, ACCESS_MAP
import app.databricks_api as dbx

app = FastAPI()


@app.post("/process")
def process_request(payload: StudyPayload):
    try:
        return process_payload(payload)
    except DatabricksAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
