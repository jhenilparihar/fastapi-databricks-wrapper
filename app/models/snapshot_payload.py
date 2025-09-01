from pydantic import BaseModel
from typing import List, Dict, Optional

class CreateSnapshotPayload(BaseModel):
    source_table_fullname: str
    product: str
    study: str
    timestamp: str
