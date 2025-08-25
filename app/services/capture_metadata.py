import json
from datetime import datetime
from typing import Dict, Any, Optional
from app.models.study_payload import StudyPayload
from app.models.metadata import MetadataLog
import psycopg2
from app.core.config import (
    LAKEBASE_DB_NAME,
    LAKEBASE_USER,
    LAKEBASE_OAUTH_TOKEN,
    LAKEBASE_HOST,
)


def insert_metadata(
    payload: StudyPayload,
    response: Dict[str, Any],
    http_status: int,
    request_by: str,
    api_response_time = float,
    error: Optional[str] = None,
    description: Optional[str] = None,
    business_justification: Optional[str] = None, 
    
):
    """
    Insert metadata record into Lakebase
    """


    log = MetadataLog(
        request_payload=payload.dict(),
        response_payload=response,
        http_status_code=http_status,
        error_message=error,
        created_at=datetime.utcnow(),
        product_name=payload.business_metadata.product_name,
        study=payload.business_metadata.study,
        study_type=payload.business_metadata.study_type,
        request_by = request_by, 
        description = description,
        business_justification = business_justification, 
        api_response_time = api_response_time
    )

    conn = psycopg2.connect(
        dbname=LAKEBASE_DB_NAME,
        user=LAKEBASE_USER,
        password=LAKEBASE_OAUTH_TOKEN,
        host=LAKEBASE_HOST,
        port="5432",
        sslmode="require",
    )
    print("Connected to Lakebase")
    cursor = conn.cursor()

    query = """
    INSERT INTO metadata (
        request_payload,
        response_payload,
        http_status_code,
        error_message,
        execution_duration_ms,
        created_at,
        product_name,
        study,
        study_type,
        request_by,
        description,
        business_justification,
        api_response_time
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    cursor.execute(
        query,
        (
            json.dumps(log.request_payload),
            json.dumps(log.response_payload),
            log.http_status_code,
            log.error_message,
            log.execution_duration_ms,
            log.created_at,
            log.product_name,
            log.study,
            log.study_type,
            log.request_by,
            log.business_justification,
            log.description,
            log.api_response_time
        ),
    )

    print("Metadata log inserted")

    conn.commit()
    cursor.close()
    conn.close()
