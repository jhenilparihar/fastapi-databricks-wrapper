from pydantic import BaseModel
from typing import List, Dict, Optional


class BusinessMetadata(BaseModel):
    product_name: str
    study: str  
    study_type: str


class VolumeDirectories(BaseModel):
    raw: List[str]
    raw_restricted: List[str]


class StorageSetup(BaseModel):
    data_schemas: List[str]
    volume_directories: VolumeDirectories


class GroupAccess(BaseModel):
    group: str
    access: str


class UserAccess(BaseModel):
    user: str
    access: str


class EntityAccessControl(BaseModel):
    groups: Optional[List[GroupAccess]] = []
    # users: Optional[List[UserAccess]] = []


# class AccessControls(BaseModel):
#     raw: Optional[EntityAccessControl]
#     raw_restricted: Optional[EntityAccessControl]
#     volumes: Optional[EntityAccessControl]


class StudyPayload(BaseModel):
    business_metadata: BusinessMetadata
    storage_setup: StorageSetup
    access_controls: Optional[Dict[str, EntityAccessControl]] = None
