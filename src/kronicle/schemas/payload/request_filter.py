# kronicle/schemas/payload/request_filter.py
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from kronicle.types.iso_datetime import IsoDateTime
from kronicle.utils.str_utils import normalize_name

DEFAULT_LIMIT = 100
DEFAULT_OFFSET = 0
MAX_LIMIT = 500
MAX_OFFSET = 10_000  # arbitrary, can adjust


class RequestFilter(BaseModel):
    """
    Encapsulates filtering options for queries.
    """

    from_date: IsoDateTime | None = Field(default=None, description="Start time (inclusive)")
    to_date: IsoDateTime | None = Field(default=None, description="End time (inclusive)")
    limit: int | None = Field(default=DEFAULT_LIMIT, description="Max number of rows to return")
    offset: int | None = Field(default=None, description="Number of rows to skip (pagination)")
    order: Literal["asc", "desc"] | None = Field(default="asc", description="Sort order: asc or desc")
    columns: list[str] | None = Field(default=None, description="Comma-separated list of columns to return")
    skip_received: bool = Field(default=True, description="False to display data reception date")

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

    # --------------------------------------------------------------------------
    # Validators
    # --------------------------------------------------------------------------
    @field_validator("to_date")
    @classmethod
    def validate_to_date(cls, v, info):
        """
        Ensure to_date is after from_date; otherwise ignore to_date.
        """
        from_date = info.data.get("from_date")
        if v is None or from_date is None:
            return v
        if IsoDateTime(v) < IsoDateTime(from_date):
            raise ValueError(f"Parameter `from_date`={from_date} must be earlier than parameter `to_date`= {v} ")
        return v

    @field_validator("columns", mode="before")
    @classmethod
    def split_fields(cls, v):
        """Allow comma-separated strings as input for convenience."""
        if isinstance(v, str):
            return [f_norm for f in v.split(",") if (f_norm := normalize_name(f))]
        return v

    # --------------------------------------------------------------------------
    # SQL generation
    # --------------------------------------------------------------------------
    def to_sql_clauses(self, start_idx: int = 1) -> tuple[str, list]:
        """
        Generate WHERE, LIMIT, OFFSET SQL fragments and parameters.

        Returns:
            sql_fragment: str
            params: list
        """
        clauses = []
        params: list = []
        idx = start_idx

        if self.from_date:
            clauses.append(f"time >= ${idx}")
            params.append(self.from_date)
            idx += 1
        if self.to_date:
            clauses.append(f"time <= ${idx}")
            params.append(self.to_date)
            idx += 1

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        limit_sql = f"LIMIT {self.limit}" if self.limit is not None else ""
        offset_sql = f"OFFSET {self.offset}" if self.offset is not None else ""

        sql_fragment = " ".join(filter(None, [where_sql, limit_sql, offset_sql]))
        return sql_fragment, params
