# kronicle/schemas/rbac/input_zone_schemas.py
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class InputZone(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Unique zone name")
    details: dict[str, Any] = Field(default_factory=dict, description="Optional JSONB metadata")
