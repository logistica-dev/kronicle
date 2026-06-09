# kronicle/schemas/rbac/safe_policy_schemas.py
from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel


class OutputZonePolicy(BaseModel):
    id: UUID
    subject_id: UUID
    role_id: UUID
    role_name: str
    zone_id: UUID
    zone_name: str
    is_delegation: bool = False


class OutputChannelPolicy(BaseModel):
    id: UUID
    subject_id: UUID
    role_id: UUID
    role_name: str
    channel_id: UUID
    channel_name: str
    is_delegation: bool = False
