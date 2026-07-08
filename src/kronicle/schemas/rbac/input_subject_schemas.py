# kronicle/schemas/rbac/input_subject_schemas.py
from __future__ import annotations

from enum import Enum
from typing import Literal
from uuid import UUID

from pydantic import Field

from kronicle.schemas.input_schema import InputSchema


class SubjectType(str, Enum):
    user = "user"
    group = "group"


class InputSubject(InputSchema):
    type: Literal["user", "group"] = Field(..., description="'user' or 'group'")
    user_id: UUID | None = Field(default=None, description="UUID of the user")
    group_id: UUID | None = Field(default=None, description="UUID of the group")
