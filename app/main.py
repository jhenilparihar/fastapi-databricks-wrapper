from tokenize import group
from fastapi import FastAPI, HTTPException, Body, Query
from app.models.study_payload import StudyPayload, Metadata #, StudySetup
from app.models.metadata import MetadataLog
from app.services.study_resources import process_payload
from app.databricks_api import DatabricksAPIError
from app.services.capture_metadata import insert_metadata
from app.services.fetch_metadata import fetch_metadata

import time

app = FastAPI()


@app.post("/process")
def process_request(payload: StudyPayload = Body(...), payload2: Metadata = Body(...)
):
    start = time.perf_counter()
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
        end = time.perf_counter()
        meta_start = time.perf_counter()
        insert_metadata(
            payload,
            response,
            http_status,
            error=None if http_status == 200 else response["message"],
            request_by = payload2.request_by,
            description = payload2.description,
            business_justification = payload2.business_justification, 
            api_response_time = (end - start) * 1000
        )
        meta_end = time.perf_counter()
        meta_duration_ms = (meta_end - meta_start) * 1000

        print(f"Insert into Lakebase (metadata) took {meta_duration_ms} ms")
        
@app.get("/get-metadata")
def get_metadata():
    """
    Fetch metadata rows from Lakebase and measure query latency
    """
    start = time.perf_counter()
    try:
        rows = fetch_metadata()
        if not rows:
            raise HTTPException(status_code=404, detail="No metadata found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching metadata: {str(e)}")
    finally:
        end = time.perf_counter()
        latency_ms = int((end - start) * 1000)
        print(f"Lakebase read operation took {latency_ms} ms")

    return {
        "latency_ms": latency_ms,
        "count": len(rows),
        "data": rows
    }
