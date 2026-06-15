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
    def from_query_filter(cls, query: TableQueryFilter, *, strict: bool = False) -> TableRequestFilter:
        feedback = OpFeedback()
        data = {}

        cls._validate_channel_id(query, data)
        cls._validate_name(query, data)
        cls._validate_date_range(query, data, feedback)
        cls._validate_tags(query, data, feedback)
        cls._validate_user_metadata(query, data, feedback)

        if feedback.has_details:
            if strict:
                raise BadRequestError("Invalid channel metadata filter", details=feedback.json())
            data["op_feedback"] = feedback

        return cls(**data)

    @classmethod
    def _validate_channel_id(cls, query: TableQueryFilter, data: dict) -> None:
        if query.id is not None:
            data["channel_id"] = query.id

    @classmethod
    def _validate_name(cls, query: TableQueryFilter, data: dict) -> None:
        if query.name is not None:
            data["name"] = query.name.strip()

    @classmethod
    def _validate_date_range(cls, query: TableQueryFilter, data: dict, feedback: OpFeedback) -> None:
        if query.date_from is not None:
            data["date_from"] = IsoDateTime(query.date_from)
        if query.date_to is not None:
            data["date_to"] = IsoDateTime(query.date_to)
        if "date_from" in data and "date_to" in data:
            if data["date_from"] > data["date_to"]:
                feedback.add_detail("from > to", "date_from")

    @classmethod
    def _validate_tags(cls, query: TableQueryFilter, data: dict, feedback: OpFeedback) -> None:
        if query.tags:
            if not isinstance(query.tags, dict):
                feedback.add_detail("tags must be a dict", "tags")
            else:
                data["tags"] = {k: v for k, v in query.tags.items() if k.strip()}

    @classmethod
    def _validate_user_metadata(cls, query: TableQueryFilter, data: dict, feedback: OpFeedback) -> None:
        if query.meta:
            if not isinstance(query.meta, dict):
                feedback.add_detail("user_metadata must be a dict", "user_metadata")
            else:
                data["user_metadata"] = query.meta
