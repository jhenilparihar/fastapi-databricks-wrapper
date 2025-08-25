import threading
import time
import uuid
from datetime import datetime
from typing import Dict, Any, Optional
import psycopg2
import json

# Simulated payload structure
class BusinessMetadata:
    def __init__(self, product_name, study, study_type):
        self.product_name = product_name
        self.study = study
        self.study_type = study_type

class StudyPayload:
    def __init__(self, business_metadata):
        self.business_metadata = business_metadata

    def dict(self):
        return {
            "business_metadata": {
                "product_name": self.business_metadata.product_name,
                "study": self.business_metadata.study,
                "study_type": self.business_metadata.study_type,
            }
        }

# DB config
LAKEBASE_DB_NAME = "your_db"
LAKEBASE_USER = "your_user"
LAKEBASE_OAUTH_TOKEN = "your_password"
LAKEBASE_HOST = "your_host"

def insert_metadata(payload: StudyPayload, response: Dict[str, Any], http_status: int,
                    error: Optional[str] = None, duration_ms: Optional[int] = None) -> str:
    unique_id = str(uuid.uuid4())
    created_at = datetime.utcnow()

    log = {
        "id": unique_id,
        "request_payload": payload.dict(),
        "response_payload": response,
        "http_status_code": http_status,
        "error_message": error,
        "execution_duration_ms": duration_ms,
        "created_at": created_at.isoformat(),
        "product_name": payload.business_metadata.product_name,
        "study": payload.business_metadata.study,
        "study_type": payload.business_metadata.study_type,
    }

    conn = psycopg2.connect(
        dbname=LAKEBASE_DB_NAME,
        user=LAKEBASE_USER,
        password=LAKEBASE_OAUTH_TOKEN,
        host=LAKEBASE_HOST,
        port="5432",
        sslmode="require",
    )
    cursor = conn.cursor()

    query = """
    INSERT INTO metadata_logs (
        id,
        request_payload,
        response_payload,
        http_status_code,
        error_message,
        execution_duration_ms,
        created_at,
        product_name,
        study,
        study_type
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(query, (
        log["id"],
        json.dumps(log["request_payload"]),
        json.dumps(log["response_payload"]),
        log["http_status_code"],
        log["error_message"],
        log["execution_duration_ms"],
        log["created_at"],
        log["product_name"],
        log["study"],
        log["study_type"],
    ))
    conn.commit()
    cursor.close()
    conn.close()
    return unique_id

def check_retrieval_lag(record_id: str, start_time: float):
    max_wait = 10  # seconds
    interval = 0.5
    elapsed = 0

    while elapsed < max_wait:
        conn = psycopg2.connect(
            dbname=LAKEBASE_DB_NAME,
            user=LAKEBASE_USER,
            password=LAKEBASE_OAUTH_TOKEN,
            host=LAKEBASE_HOST,
            port="5432",
            sslmode="require",
        )
        cursor = conn.cursor()
        cursor.execute("SELECT created_at FROM metadata_logs WHERE id = %s", (record_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result:
            lag_ms = int((time.time() - start_time) * 1000)
            print(f"✅ Record {record_id} retrieved after {lag_ms} ms")
            return
        time.sleep(interval)
        elapsed += interval

    print(f"❌ Record {record_id} not found after {max_wait} seconds")

def simulate_user(index: int):
    payload = StudyPayload(BusinessMetadata(
        product_name=f"Product-{index}",
        study=f"Study-{index}",
        study_type="Type-A"
    ))
    response = {"status": "success", "message": "Inserted"}
    http_status = 200
    start = time.time()
    record_id = insert_metadata(payload, response, http_status, duration_ms=0)
    check_retrieval_lag(record_id, start)

if __name__ == "__main__":
    threads = []
    user_count = 10  # Simulate 10 concurrent users

    for i in range(user_count):
        t = threading.Thread(target=simulate_user, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()