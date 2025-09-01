from pydantic import BaseModel, EmailStr
from typing import List, Dict


class BusinessMetadata(BaseModel):
    product_name: str
    study: str
    analysis_lead: EmailStr
    analysis_type: str


class StorageSetup(BaseModel):
    volume_directories: List[str]
    data_layer_schemas: List[str]


class AccessControl(BaseModel):
    group: str
    table_actions: str
    volume_action: str
    business_action: List[str]


class AnalysisPayload(BaseModel):
    business_metadata: BusinessMetadata
    storage_setup: StorageSetup
    access_controls: Dict[str, AccessControl]
