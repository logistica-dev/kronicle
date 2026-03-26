# kronicle/schemas/filters/row_fetch_context.py
from __future__ import annotations

from typing import Any

from fastapi._compat.v2 import normalize_name
from pydantic import BaseModel, PrivateAttr

from kronicle.db.data.models.schema_types import SchemaType
from kronicle.errors.error_types import BadRequestError
from kronicle.schemas.filters.col_filters import (
    AnyFilter,
    ColumnFilter,
    ExactFilter,
    HasFilter,
    MaxFilter,
    MinFilter,
    OrderBy,
)
from kronicle.schemas.filters.row_request_filter import RowRequestFilter
from kronicle.schemas.payload.op_feedback import OpFeedback
from kronicle.utils.dev_logs import log_block, log_d, log_w, setup_logging
from kronicle.utils.str_utils import split_strip


# -------------------------------------------------------------------------------------------------
# RowFetchContext
# -------------------------------------------------------------------------------------------------
class RowFetchContext(BaseModel):
    """
    DB-aware context for fetching rows.

    Transforms and validates a RowRequestFilter into a fully-resolved,
    SQL-ready object.

    Responsibilities:
    - Normalized column names
    - Resolved dotted paths for dict columns
    - Min/max/any filters
    - SQL generation with proper type casting
    """

    column_types: dict[str, SchemaType]
    req_filters: RowRequestFilter

    _filter_map: dict[str, type[ColumnFilter]] = {
        "col": ExactFilter,
        "min": MinFilter,
        "max": MaxFilter,
        "any": AnyFilter,
        "has": HasFilter,
    }

    _feedback: OpFeedback = PrivateAttr(default_factory=OpFeedback)

    _filters: list[ColumnFilter] = PrivateAttr(default_factory=list)
    _sort: list[OrderBy] = PrivateAttr(default_factory=list)

    def model_post_init(self, __context: Any) -> None:
        here = "model_post_init"
        with log_block(here, "resolving filters"):
            self._resolve_filters()
        with log_block(here, "resolving sort"):
            self._resolve_sort()

        if self._feedback.has_details and self.req_filters.strict:
            raise BadRequestError("Incorrect query parameters", details=self._feedback.json())

    @property
    def limit(self) -> int | None:
        return self.req_filters.limit

    @property
    def offset(self) -> int | None:
        return self.req_filters.offset

    @property
    def feedback(self) -> OpFeedback:
        """Return the operation feedback object, accumulating warnings/errors."""
        return self._feedback

    def _resolve_filters(self):
        for attr, ColFilter in self._filter_map.items():
            values = getattr(self.req_filters, attr)
            for col, val in values.items():
                try:
                    col_name, *subkeys = split_strip(col, ".", normalize_name)
                    if col_name not in self.column_types:
                        raise ValueError(f"Unknown column '{col_name}'")
                    col_filter = ColFilter.factory(
                        col_type=self.column_types[col_name],
                        col_name=col_name,
                        subkeys=subkeys,
                        value=val,
                    )
                    self._filters.append(col_filter)
                except ValueError as e:
                    self._feedback.add_detail("Incorrect filter", f"request.{attr}", f"{col}={val}", extra=str(e))

    def _resolve_sort(self):
        here = "_resolve_sort"
        sort_cols = self.req_filters.sort
        if not sort_cols or not isinstance(sort_cols, list):
            return

        for col in sort_cols:
            desc = col[0] == "-"
            col_name = col[1:] if desc else col
            try:
                if col_name not in self.column_types:
                    raise ValueError(f"Unknown column '{col}'")
                self._sort.append(
                    OrderBy.factory(
                        col_type=self.column_types[col_name],
                        col_name=col_name,
                        desc=desc,
                    )
                )
            except ValueError as e:
                log_w(here, e)
                self._feedback.add_detail(str(e), "request.sort", col)

    def to_sql(self, order_by: str | None = None, desc: bool = True) -> tuple[str, list]:
        """
        Generate full SQL fragment with WHERE, ORDER BY, LIMIT, OFFSET.

        Handles:
          - Exact match filters (self.col)
          - Min/max filters (self.min, self.max)
          - Multi-value 'any' filters
          - JSON/dict path columns
          - Type casting for min/max

        Args:
            order_by: Optional column to order by
            desc: True for DESC, False for ASC

        Returns:
            Tuple of (SQL fragment string, list of parameters)
        """
        clauses: list[str] = []
        params: list[Any] = []
        idx = 1

        for f in self._filters:
            sql, p = f.to_sql(idx)
            clauses.append(sql)
            params.extend(p)
            idx += len(p)

        # Assemble final SQL
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        order_sql = f"ORDER BY {' '.join(s.sql for s in self._sort)}" if self._sort else ""
        limit_sql = f"LIMIT {self.limit}" if self.limit else ""
        offset_sql = f"OFFSET {self.offset}" if self.offset else ""
        sql_fragment = " ".join(filter(None, [where_sql, order_sql, limit_sql, offset_sql]))
        return sql_fragment, params


if __name__ == "__main__":  # pragma: no cover
    here = "row_fetch_context.tests"
    setup_logging()

    f = RowRequestFilter(
        col={"status": "ok", "author.name": "Alice"},
        min={"temperature": "20"},
        max={"temperature": "30"},
        any={"author.surname": ["Hopkins", "Yanda"]},
        has={"tags": ["room3"]},
        sort=["temperature", "-time"],
        limit=10,
        offset=5,
    )
    column_types = {
        "temperature": SchemaType("float"),
        "status": SchemaType("str"),
        "tags": SchemaType("list"),
        "author": SchemaType("dict"),
        "time": SchemaType("datetime"),
    }
    ctx = RowFetchContext(req_filters=f, column_types=column_types)
    sql, params = ctx.to_sql()
    log_d(here, sql, params)
