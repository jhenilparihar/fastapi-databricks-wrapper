from tokenize import group
from fastapi import FastAPI, HTTPException, Body, Query
from app.models.study_payload import StudyPayload, Metadata
from app.models.snapshot_payload import CreateSnapshotPayload
from app.models.metadata import MetadataLog
from app.services.study_resources import process_payload
from app.services.create_snapshot import create_snapshot
from app.databricks_api import DatabricksAPIError
from app.services.capture_metadata import insert_metadata
from app.services.fetch_metadata import fetch_metadata
from app.core.logging_config import get_logger
import time

app = FastAPI()
logger = get_logger("main")


@app.post("/process")
def process_request(payload: StudyPayload = Body(...), payload2: Metadata = Body(...)):
    logger.info(f"Incoming payload for processing: {payload.dict()}")

    start = time.perf_counter()
    response = None
    http_status = None
    try:
        response = process_payload(payload)
        http_status = 200

        # Log success with full response
        logger.info(
            {
                "event": "process_request_success",
                "status_code": http_status,
                "response": response,
            }
        )

        return response

    except DatabricksAPIError as e:
        response = {"status": "Databricks Error", "message": str(e)}
        http_status = e.status_code

        # Log Databricks error details
        logger.error(
            {
                "event": "process_request_error",
                "error_type": "DatabricksAPIError",
                "status_code": http_status,
                "error_message": e.message,
                "response": response,
            }
        )

        raise HTTPException(status_code=http_status, detail=e.message)

    except Exception as e:
        response = {"status": "Error", "message": str(e)}
        http_status = 500

        # Log unhandled exception details
        logger.exception(
            {
                "event": "process_request_exception",
                "status_code": http_status,
                "error_message": str(e),
                "response": response,
            }
        )

        raise HTTPException(status_code=500, detail=str(e))

    finally:
        end = time.perf_counter()
        meta_start = time.perf_counter()
        insert_metadata(
            payload,
            response,
            http_status,
            error=None if http_status == 200 else response["message"],
            request_by=payload2.request_by,
            description=payload2.description,
            business_justification=payload2.business_justification,
            api_response_time=(end - start) * 1000,
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
        raise HTTPException(
            status_code=500, detail=f"Error fetching metadata: {str(e)}"
        )
    finally:
        end = time.perf_counter()
        latency_ms = int((end - start) * 1000)
        print(f"Lakebase read operation took {latency_ms} ms")

    return {"latency_ms": latency_ms, "count": len(rows), "data": rows}


@app.post("/create-snapshot")
def create_snpshot(payload: CreateSnapshotPayload):
    """
    Create a snapshot of a table at a specific timestamp
    """
    try:
        # Assuming dbx.create_snapshot is a function that creates the snapshot
        result = create_snapshot(payload)
        return {"status": "Snapshot created successfully", "details": result}
    except DatabricksAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
