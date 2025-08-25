from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime


class MetadataLog(BaseModel):
    id: Optional[int] = None  # DB generated, can skip in insert
    request_payload: Dict[str, Any]      # full StudyPayload as dict/json
    response_payload: Optional[Dict[str, Any]] = None
    http_status_code: int
    error_message: Optional[str] = None
    execution_duration_ms: Optional[int] = None
    created_at: datetime = datetime.utcnow()

    # derived fields (from business_metadata in StudyPayload)
    product_name: Optional[str] = None
    study: Optional[str] = None
    study_type: Optional[str] = None
