# kronicle/schemas/filters/table_request_filter.py
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from kronicle.errors.error_types import BadRequestError
from kronicle.schemas.filters.table_query_filter import TableQueryFilter
from kronicle.schemas.payload.op_feedback import OpFeedback
from kronicle.types.iso_datetime import IsoDateTime


class TableRequestFilter(BaseModel):
    channel_id: UUID | None = None
    name: str | None = None
    date_from: IsoDateTime | None = None
    date_to: IsoDateTime | None = None
    tags: dict[str, Any] = Field(default_factory=dict)
    user_metadata: dict[str, Any] = Field(default_factory=dict)
    op_feedback: OpFeedback

    @classmethod
    def from_query_filter(cls, query: TableQueryFilter, strict: bool = False) -> TableRequestFilter:
        feedback = OpFeedback()
        data = {}

        # Validate channel_id
        if query.id is not None:
            data["channel_id"] = query.id

        # Validate name
        if query.name is not None:
            data["name"] = query.name.strip()

        # Validate received_at range
        if query.date_from is not None:
            data["date_from"] = IsoDateTime(query.date_from)
        if query.date_to is not None:
            data["date_to"] = IsoDateTime(query.date_to)
        if "date_from" in data and "date_to" in data:
            if data["date_from"] > data["date_to"]:
                feedback.add_detail("from > to", "date_from")

        # Validate tags
        if query.tags:
            if not isinstance(query.tags, dict):
                feedback.add_detail("tags must be a dict", "tags")
            else:
                data["tags"] = {k: v for k, v in query.tags.items() if k.strip()}

        # Validate user_metadata
        if query.meta:
            if not isinstance(query.meta, dict):
                feedback.add_detail("user_metadata must be a dict", "user_metadata")
            else:
                data["user_metadata"] = query.meta

        if feedback.has_details:
            if strict:
                raise BadRequestError("Invalid channel metadata filter", details=feedback.json())
            data["op_feedback"] = feedback

        return cls(**data)
