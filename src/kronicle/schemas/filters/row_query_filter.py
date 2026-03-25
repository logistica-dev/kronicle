# kronicle/schemas/filters/row_query_filter.py
from typing import Literal

from fastapi import Query
from pydantic import BaseModel, PrivateAttr, field_validator, model_validator

from kronicle.schemas.payload.op_feedback import OpFeedback
from kronicle.utils.str_utils import normalize_name, normalize_query_name

DEFAULT_LIMIT = 100
DEFAULT_OFFSET = 0
MAX_LIMIT = 500
MAX_OFFSET = 10_000  # arbitrary, can adjust
DEFAULT_STRICT_MODE = False


class RowQueryFilter(BaseModel):
    """
    Human-friendly query filter for rows/columns.

    URL syntax examples:
        ?col[name]=Tintin
        ?min[temperature]=20
        ?max[temperature]=30
        ?list[tag]=room1,room2
    """

    # Row-level pagination/sorting
    limit: int | None = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT, description="Max rows to return")
    offset: int | None = Query(None, ge=0, le=MAX_OFFSET, description="Number of rows to skip")
    order: Literal["asc", "desc"] = Query("asc", description="Sort order")

    # Column selection
    columns: list[str] | None = Query(None, description="Columns to include in the result")
    skip_received: bool = Query(True, description="False to include reception timestamp")

    # Row filters
    col: dict[str, str] = Query(default_factory=dict, description="Exact match filters: ?col[name]=Tintin")
    min: dict[str, str] = Query(default_factory=dict, description="Minimum value filters: ?min[time]=2026-03-20")
    max: dict[str, str] = Query(default_factory=dict, description="Maximum value filters: ?max[time]=2026-03-21")
    any: dict[str, str] = Query(default_factory=dict, description="Multi-value filters: ?any[status]=ok,fail")

    # Strict mode and feedback
    strict: bool = Query(DEFAULT_STRICT_MODE, description="Raise errors if true, otherwise accumulate warnings")
    _feedback: OpFeedback = PrivateAttr(default_factory=OpFeedback)

    @property
    def feedback(self) -> OpFeedback:
        return self._feedback

    # --------------------------------------------------------------------------
    # Normalization
    # --------------------------------------------------------------------------
    @field_validator("limit", mode="before")
    @classmethod
    def cap_limit(cls, v):
        if v is None or v < 1:
            return DEFAULT_LIMIT
        return min(v, MAX_LIMIT)

    @field_validator("offset", mode="before")
    @classmethod
    def cap_offset(cls, v):
        if v is None:
            return None
        if v < 1:
            v = DEFAULT_OFFSET
        return min(v, MAX_OFFSET)

    @field_validator("columns", mode="before")
    @classmethod
    def split_column_selection(cls, v):
        if v is None:
            return None
        if isinstance(v, str):
            return [normalize_name(stripped_c) for c in v.split(",") if (stripped_c := c.strip()) != ""]
        if isinstance(v, list):
            return v
        return None

    def process_filter(self, name: str, filter: dict[str, str] | None) -> dict[str, str]:
        if not filter:
            return {}

        if not isinstance(filter, dict):
            self._feedback.add_detail(message="Expected a dict", field=name)
            return {}

        normalized = {}
        for key, val in filter.items():
            try:
                norm_key = normalize_query_name(key)
                if not norm_key:
                    raise ValueError("Empty key after normalization")
                normalized[norm_key] = val
            except Exception:
                self._feedback.add_detail(message="Invalid column name", field=name, subfield=str(key))
        return normalized

    @model_validator(mode="after")
    def ensure_column_names(self):
        self.col = self.process_filter("col", self.col)
        self.min = self.process_filter("min", self.min)
        self.max = self.process_filter("max", self.max)
        self.any = self.process_filter("any", self.any)

        return self
