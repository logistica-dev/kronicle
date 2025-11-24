# kronicle/types/query_params.py
from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator


class RowQueryParams(BaseModel):
    from_date: Optional[datetime] = Field(None, description="Start time (inclusive)")
    to_date: Optional[datetime] = Field(None, description="End time (inclusive)")
    limit: Optional[int] = Field(None, ge=1, description="Max number of rows to return")
    offset: Optional[int] = Field(0, ge=0, description="Number of rows to skip (pagination)")
    order: Literal["asc", "desc"] = Field("asc", description="Sort order: asc or desc")
    columns: Optional[list[str]] = Field(None, description="Comma-separated list of columns to return")

    @field_validator("columns", mode="before")
    @classmethod
    def split_fields(cls, v):
        """Allow comma-separated strings as input for convenience."""
        if isinstance(v, str):
            return [f.strip() for f in v.split(",") if f.strip()]
        return v
