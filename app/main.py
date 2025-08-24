from tokenize import group
from fastapi import FastAPI, HTTPException
from app.schemas.study_payload import StudyPayload
from app.services.study_resources import process_payload
from app.databricks_api import DatabricksAPIError
from app.services.capture_metadata import insert_metadata

import time

app = FastAPI()


@app.post("/process")
def process_request(payload: StudyPayload):
    start = time.time()

    try:
        response = process_payload(payload)
        http_status = 200
        return response
    except DatabricksAPIError as e:
        response = {"status": "Databricks Error", "message": str(e)}
        http_status = e.status_code
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        response = {"status": "Error", "message": str(e)}
        http_status = 500
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        duration_ms = int((time.time() - start) * 1000)
        insert_metadata(
            payload,
            response,
            http_status,
            error=None if http_status == 200 else response["message"],
            duration_ms=duration_ms,
        )
