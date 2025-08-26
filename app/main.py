from tokenize import group
from fastapi import FastAPI, HTTPException
from app.models.study_payload import StudyPayload
from app.services.study_resources import process_payload
from app.databricks_api import DatabricksAPIError
from app.services.capture_metadata import insert_metadata
from app.core.logging_config import get_logger
import time

app = FastAPI()
logger = get_logger("main")

@app.post("/process")
def process_request(payload: StudyPayload):
    logger.info(f"Incoming payload for processing: {payload.dict()}")

    start = time.time()
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
        duration_ms = int((time.time() - start) * 1000)

        # metadata in DB
        insert_metadata(
            payload,
            response,
            http_status,
            error=None if http_status == 200 else response["message"],
            duration_ms=duration_ms,
        )

        # log request duration
        logger.info(
            {
                "event": "process_request_completed",
                "status_code": http_status,
                "duration_ms": duration_ms,
            }
        )
