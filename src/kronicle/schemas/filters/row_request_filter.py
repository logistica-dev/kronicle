# kronicle/schemas/filters/row_request_filter.py
from __future__ import annotations

from pydantic import BaseModel, Field, PrivateAttr

from kronicle.schemas.filters.row_query_filter import DEFAULT_LIMIT, DEFAULT_STRICT_MODE, RowQueryFilter
from kronicle.schemas.payload.op_feedback import OpFeedback


class RowRequestFilter(BaseModel):
    """
    Validated and safe-to-use filter with SQL generation support.
    """

    # Row-level pagination/sorting
    limit: int | None = Field(default=DEFAULT_LIMIT, description="Max number of rows to return")
    offset: int | None = Field(default=None, description="Number of rows to skip (pagination)")
    sort: list[str] | None = Field(default=None, description="Sort columns, prefix with '-' for descending order")

    # Column selection
    columns: list[str] | None = Field(default=None, description="Comma-separated list of columns to return")
    skip_received: bool = Field(default=True, description="False to display data reception date")

    # Row filters
    col: dict[str, str] = Field(default_factory=dict, description="Exact match filters: ?col[author.name]=Herbert")
    min: dict[str, str] = Field(default_factory=dict, description="Minimum value filters: ?min[time]=2026-03-20")
    max: dict[str, str] = Field(default_factory=dict, description="Maximum value filters: ?max[time]=2026-03-21")
    any: dict[str, list[str]] = Field(default_factory=dict, description="Multi-value filters: ?any[tags]=room1,room2")
    has: dict[str, list[str]] = Field(default_factory=dict, description="Multi-value filters: ?any[tags]=room1,room2")

    # Strict mode and feedback
    strict: bool = Field(default=DEFAULT_STRICT_MODE, description="Raise errors if true, otherwise accumulate warnings")
    _feedback: OpFeedback = PrivateAttr(default_factory=OpFeedback)

    @property
    def feedback(self) -> OpFeedback:
        return self._feedback

    # --------------------------------------------------------------------------
    # Copy
    # --------------------------------------------------------------------------
    def copy_with_feedback(self) -> RowRequestFilter:
        new = self.model_copy()
        new._feedback = self._feedback
        return new

    # --------------------------------------------------------------------------
    # Factory
    # --------------------------------------------------------------------------
    @classmethod
    def from_query(cls, query_filter: RowQueryFilter) -> RowRequestFilter:
        """
        Convert the query filter into a domain RequestFilter.
        Handles splitting columns string into a list.
        """
        new = RowRequestFilter(**query_filter.model_dump())
        new._feedback = query_filter.feedback
        return new
