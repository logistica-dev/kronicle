# kronicle/schemas/filters/row_fetch_context.py
from __future__ import annotations

from kronicle.db.data.models.schema_types import SchemaType
from kronicle.errors.error_types import BadRequestError
from kronicle.schemas.filters.col_filters import (
    AnyFilter,
    ColumnFilter,
    ExactFilter,
    MaxFilter,
    MinFilter,
    ResolvedColumn,
)
from kronicle.schemas.filters.row_request_filter import RowRequestFilter
from kronicle.schemas.payload.op_feedback import OpFeedback
from kronicle.utils.dev_logs import log_d, setup_logging
from kronicle.utils.str_utils import normalize_pg_identifier


# -------------------------------------------------------------------------------------------------
# RowFetchContext
# -------------------------------------------------------------------------------------------------
class RowFetchContext:
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

    def __init__(self, filter: RowRequestFilter, column_types: dict[str, SchemaType]):
        """
        Initialize the context.

        Args:
            filter: The RowRequestFilter input from user/API.
            column_types: Mapping of column names to their SchemaType.
        """
        self._filter = filter
        self._column_types = column_types
        self._feedback: OpFeedback = filter.feedback

        # Resolved columns
        self.filters: list[ColumnFilter] = []

        self._resolve_filters()

        if self._feedback.has_details and self._filter.strict:
            raise BadRequestError("Incorrect query parameters", details=self._feedback.json())

    @property
    def feedback(self) -> OpFeedback:
        """Return the operation feedback object, accumulating warnings/errors."""
        return self._feedback

    def _resolve_filters(self):
        mapping = [
            ("col", ExactFilter),
            ("min", MinFilter),
            ("max", MaxFilter),
            ("any", AnyFilter),
        ]

        for attr, cls in mapping:
            values = getattr(self._filter, attr)
            for key, value in values.items():
                try:
                    col_name, *subkeys = key.split(".")
                    if col_name not in self._column_types:
                        raise ValueError(f"Unknown column '{col_name}'")
                    col_type = self._column_types[col_name]
                    if subkeys and col_type.name != "dict":
                        raise ValueError(f"Column '{key}' has subpath but is not a dict")
                    resolved_col = ResolvedColumn(col_type=col_type, col_name=col_name, subkeys=subkeys)
                    self.filters.append(cls(resolved_col, value))
                except ValueError as e:
                    self._feedback.add_detail(str(e), "request_filter", key)

    def _validate_all_columns(self) -> None:
        """
        Resolve all filter columns and store them in self.col, self.min, etc.

        Accumulates feedback details for any unknown or invalid columns.
        """
        for attr in ["col", "min", "max", "any"]:
            fdict = getattr(self._filter, attr)
            resolved_dict = {}
            for col in fdict.keys():
                try:
                    resolved_dict[col] = self._resolve_column(col)
                except ValueError as e:
                    self._feedback.add_detail(str(e), "request_filter", col)
            setattr(self, attr, resolved_dict)

    # -------------------------------------------------------------------------------------------------------------------------
    # SQL generation helpers
    # -------------------------------------------------------------------------------------------------------------------------
    def _cast_for_json_type(self, col_type: SchemaType, *, op: str = "=") -> str:
        """
        Return SQL cast for JSON/dict values depending on operation and type.

        Args:
            col_type: SchemaType of the column.
            op: SQL operator ("=", ">=", "<=", etc).

        Returns:
            A string representing the SQL cast (e.g., "::numeric" or "::timestamptz"), or empty.
        """
        if op in {">=", "<="}:
            if col_type.name in {"int", "float"}:
                return "::numeric"
            if col_type.name in {"datetime", "timetz", "timestamptz"}:
                return "::timestamptz"
        return ""

    def _sql_expr_for_value(
        self, resolved: ResolvedColumn, value: str | list, *, op: str = "=", idx: int = 1
    ) -> tuple[str, list]:
        """
        Return SQL expression and parameter list for a resolved column.

        Args:
            resolved: ResolvedColumn object
            value: The value or list of values to compare
            op: SQL operator
            idx: Placeholder index for numbered SQL parameters

        Returns:
            Tuple[sql_expression, parameter_list]
        """
        cast_sql = self._cast_for_json_type(resolved.col_type, op=op)
        if resolved.is_json:
            path_sql, last_key = resolved.json_path_sql
            sql_col = resolved.sql_identifier
            sql = f"{sql_col}{'->' + path_sql if path_sql else ''}->>%s {op} ${idx}{cast_sql}"
            return sql, [last_key, value]
        else:
            sql_col = resolved.sql_identifier
            if isinstance(value, list):
                placeholders = ",".join(f"${i}" for i in range(idx, idx + len(value)))
                sql = f"{sql_col} IN ({placeholders}){cast_sql}"
                return sql, value
            else:
                sql = f"{sql_col} {op} ${idx}{cast_sql}"
                return sql, [value]

    def _process_filter_items(
        self, fdict: dict[str, ResolvedColumn], values: dict[str, str], *, op: str = "=", idx: int = 1
    ) -> tuple[list[str], list, int]:
        """
        Process a dictionary of resolved columns into SQL clauses.

        Args:
            fdict: Mapping of user keys to ResolvedColumn objects
            values: Corresponding values to filter
            op: SQL operator
            idx: Placeholder index for numbered SQL parameters

        Returns:
            Tuple[clauses, parameters, next_placeholder_index]
        """
        clauses = []
        params = []
        for key, resolved in fdict.items():
            val = values[key]
            expr, typed_values = self._sql_expr_for_value(resolved, val, op=op, idx=idx)
            clauses.append(expr)
            if isinstance(val, list):
                idx += len(val)
            else:
                idx += 1
            params.extend(typed_values)
        return clauses, params, idx

    def to_sql_clauses(self, order_by: str | None = None, desc: bool = True) -> tuple[str, list]:
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
        clauses = []
        params = []
        idx = 1

        # Exact, min, max filters
        for fdict_name, op in [("col", "="), ("min", ">="), ("max", "<=")]:
            fdict = getattr(self, fdict_name)
            values = getattr(self._filter, fdict_name)
            f_clauses, f_params, idx = self._process_filter_items(fdict, values, op=op, idx=idx)
            clauses.extend(f_clauses)
            params.extend(f_params)

        # 'any' filters (multi-value)
        any_dict = self.any
        any_values = self._filter.any
        for key, resolved in any_dict.items():
            val = any_values[key]
            if not isinstance(val, list):
                val_list = [v.strip() for v in val.split(",")]
            else:
                val_list = val
            expr, typed_values = self._sql_expr_for_value(resolved, val_list, op="=", idx=idx)
            clauses.append(expr)
            params.extend(typed_values)
            idx += len(val_list)

        # Assemble final SQL
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        order_sql = f"ORDER BY {normalize_pg_identifier(order_by)} {'DESC' if desc else 'ASC'}" if order_by else ""
        limit_sql = f"LIMIT {self._filter.limit}" if self._filter.limit else ""
        offset_sql = f"OFFSET {self._filter.offset}" if self._filter.offset else ""
        sql_fragment = " ".join(filter(None, [where_sql, order_sql, limit_sql, offset_sql]))
        return sql_fragment, params


if __name__ == "__main__":  # pragma: no cover
    here = "row_fetch_context.tests"
    setup_logging()

    f = RowRequestFilter(
        col={"status": "ok", "author.name": "Alice"},
        min={"temperature": "20"},
        max={"temperature": "30"},
        any={"tags": "room1,room2"},
        limit=10,
        offset=5,
    )
    column_types = {
        "temperature": SchemaType("float"),
        "status": SchemaType("str"),
        "tags": SchemaType("dict"),
        "author": SchemaType("dict"),
        "time": SchemaType("datetime"),
    }
    # ctx = RowFetchContext(f, column_types=column_types)
    # sql, params = ctx.to_sql_clauses(order_by="time")
    # log_d(here, sql, params)

    col_int = ResolvedColumn(col_type=SchemaType("int"), col_name="count", subkeys=[])
    f = AnyFilter(col_int, "4,5,6")
    sql, params = f.to_sql(1)
    log_d(here, sql, params)
