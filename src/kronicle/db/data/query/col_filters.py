# kronicle/db/data/query/col_filters.py
from __future__ import annotations

from dataclasses import dataclass
from json import dumps
from typing import Any

from kronicle.db.data.models.schema_types import SchemaType
from kronicle.utils.dev_logs import log_d, log_e
from kronicle.utils.str_utils import normalize_name, normalize_pg_identifier, split_strip


# -------------------------------------------------------------------------------------------------
# Resolved column representation
# -------------------------------------------------------------------------------------------------
@dataclass(slots=True)
class ResolvedColumn:
    col_type: SchemaType
    col_name: str
    subkeys: list[str] | None = None

    def __post_init__(self):
        self.col_name = normalize_pg_identifier(self.col_name)
        if self.subkeys:
            self.subkeys = [normalize_name(k) for k in self.subkeys]

        if not self.subkeys and self.col_type.name == "dict":
            raise ValueError(
                f"No filter allowed for full JSON field '{self.col_name}', only with subkeys like `author.name`"
            )

        if self.subkeys and self.col_type.name != "dict":
            raise ValueError(
                f"Column '{self.col_name}' of type '{self.col_type.name}' cannot have subkeys {self.subkeys}"
            )

    @property
    def is_collection(self) -> bool:
        """
        Returns true if this is a list or dict
        """
        return self.col_type.name in ["dict", "list"]

    @property
    def is_json(self) -> bool:
        return bool(self.subkeys)

    @property
    def json_path_sql(self) -> tuple[str, str]:
        if not self.subkeys:
            return "", ""
        path = "->".join(f"'{k}'" for k in self.subkeys[:-1])
        last_key = self.subkeys[-1]
        return path, last_key


# -------------------------------------------------------------------------------------------------
# Base filter
# -------------------------------------------------------------------------------------------------
@dataclass(slots=True)
class ColumnFilter:
    op: str  # SQL operator
    col: ResolvedColumn
    value: Any

    def __init__(self, resolved_col: ResolvedColumn, value: Any):
        self.col = resolved_col
        self.value = value

    @classmethod
    def factory(cls, col_type: SchemaType, col_name: str, subkeys: list[str], value: Any):
        return cls(ResolvedColumn(col_type=col_type, col_name=col_name, subkeys=subkeys), value)

    @property
    def _sql_params(self) -> list[Any]:
        return [self.value]

    @property
    def type_name(self) -> str:
        return self.col.col_type.name

    @property
    def col_name(self) -> str:
        return self.col.col_name

    def _condition(self, idx: int) -> str:
        return f"{self.op} ${idx}"

    def to_sql(self, idx: int) -> tuple[str, list[Any]]:
        condition = self._condition(idx)

        if self.col.is_json:
            path_sql, last_key = self.col.json_path_sql
            sql = f"({self.col_name}{'->' + path_sql if path_sql else ''}->>'{last_key}') {condition}"
            return sql, self._sql_params
        return f"{self.col_name} {condition}", self._sql_params


# -------------------------------------------------------------------------------------------------
# Range filter types
# -------------------------------------------------------------------------------------------------
class RangeFilter(ColumnFilter):
    def __init__(self, resolved_col: ResolvedColumn, value: Any):
        if resolved_col.col_type.name == "dict":
            raise ValueError(
                f"Range filters (min/max) not supported on JSON field "
                f"'{resolved_col.col_name}.{'.'.join(resolved_col.subkeys) if resolved_col.subkeys else ''}'"
            )
        if resolved_col.col_type.name == "list":
            raise ValueError(f"Range filters (min/max)  not supported on list field '{resolved_col.col_name}'")
        super().__init__(resolved_col, resolved_col.col_type.normalize_value(value))

    @property
    def sql_cast(self) -> str:
        if self.type_name in {"int", "float"}:
            return "::numeric"
        if self.type_name in {"datetime", "timestamptz", "timetz"}:
            return "::timestamptz"
        return ""

    def to_sql(self, idx: int) -> tuple[str, list[Any]]:
        return f"{self.col_name}{self.sql_cast} {self._condition(idx)}", self._sql_params


class MinFilter(RangeFilter):
    op = ">="


class MaxFilter(RangeFilter):
    op = "<="


# -------------------------------------------------------------------------------------------------
# Exact filter
# -------------------------------------------------------------------------------------------------
class ExactFilter(ColumnFilter):
    op = "="

    def __init__(self, resolved_col: ResolvedColumn, value: Any):
        if resolved_col.col_type.name == "list":
            raise ValueError(f"Filter 'col' not supported on list field '{resolved_col.col_name}")
        if resolved_col.col_type.name == "dict":
            if not resolved_col.subkeys:
                raise ValueError(
                    f"Filter 'col' not supported on JSON field "
                    f"'{resolved_col.col_name}.{'.'.join(resolved_col.subkeys) if resolved_col.subkeys else ''}'"
                )
            super().__init__(resolved_col, str(value))
        else:
            super().__init__(resolved_col, resolved_col.col_type.normalize_value(value))


# -------------------------------------------------------------------------------------------------
# Any filter
# -------------------------------------------------------------------------------------------------
class AnyFilter(ColumnFilter):
    op = "IN"
    value: list[Any]

    def __init__(self, resolved_col: ResolvedColumn, value: list[str]):
        here = "AnyFilter"
        if resolved_col.col_type.name == "list":
            raise ValueError(f"Filter 'any' not supported on list field '{resolved_col.col_name}")
        if resolved_col.col_type.name == "dict":
            if not resolved_col.subkeys:
                raise ValueError(
                    f"Filter 'any' not supported on JSON field "
                    f"'{resolved_col.col_name}.{'.'.join(resolved_col.subkeys) if resolved_col.subkeys else ''}'"
                )
            values = value
        elif isinstance(value, str):
            log_d(here, resolved_col.col_name, f"is {resolved_col.col_type.name}")
            values = split_strip(value, ",", norm=resolved_col.col_type.normalize_value)
            log_d(here, "splitted values:", values)

        elif isinstance(value, (list, tuple)):
            if not resolved_col.is_collection:
                values = [norm_val for v in value if (norm_val := resolved_col.col_type.normalize_value(v))]
            else:
                values = [str(v) for v in value]
        else:
            raise ValueError("AnyFilter expects a list or tuple of values")
        super().__init__(resolved_col, values)

    @property
    def _sql_params(self) -> list[Any]:
        return self.value

    def _condition(self, idx: int) -> str:
        placeholders = ",".join(f"${i}" for i in range(idx, idx + len(self.value)))
        return f"{self.op} ({placeholders})"


# -------------------------------------------------------------------------------------------------
# Has filter (for list-typed columns)
# -------------------------------------------------------------------------------------------------
class HasFilter(ColumnFilter):
    op = "@>"

    def __init__(self, resolved_col: ResolvedColumn, value: Any):
        if resolved_col.col_type.name != "list":
            raise ValueError(f"HasFilter only applies to list-typed columns, got '{resolved_col.col_type.name}'")
        if isinstance(value, str):
            values = split_strip(value, ",", normalize_name)
        elif isinstance(value, (list, tuple)):
            values = [normalize_name(v) for v in value]
        else:
            raise ValueError("HasFilter expects a list or tuple of values")
        super().__init__(resolved_col, values)

    @property
    def _sql_params(self) -> list[Any]:
        return [dumps(self.value)]

    def _condition(self, idx: int) -> str:
        return f"{self.op} ${idx}::jsonb"

    def to_sql(self, idx: int) -> tuple[str, list[Any]]:
        sql_col = self.col_name
        if self.col.is_json:
            path_sql, last_key = self.col.json_path_sql
            sql_col_expr = f"({sql_col}{'->' + path_sql if path_sql else ''}->>'{last_key}')::jsonb"
        else:
            sql_col_expr = f"{sql_col}::jsonb"
        sql = f"{sql_col_expr} {self._condition(idx)}"
        return sql, self._sql_params


@dataclass(slots=True)
class OrderBy:
    col: ResolvedColumn
    desc: bool = False

    def __post_init__(self):
        # Same as RangeFilter, no ordering for collections as we cannot safely cast values
        if self.col.is_collection:
            raise ValueError(f"Cannot sort on collection column '{self.col.col_name}'")

    @property
    def sql(self) -> str:
        return f"{self.col.col_name} {'DESC' if self.desc else 'ASC'}"

    @classmethod
    def factory(cls, col_type: SchemaType, col_name: str, desc: bool):
        return cls(col=ResolvedColumn(col_type=col_type, col_name=col_name), desc=desc)


if __name__ == "__main__":  # pragma: no cover
    here = "row_fetch_context.tests"

    col_int = ResolvedColumn(col_type=SchemaType("int"), col_name="count")
    f = AnyFilter(col_int, ["4", "5", "6"])
    log_d(here, f.__class__.__name__)
    sql, params = f.to_sql(1)
    log_d(here, sql, params)

    col = ResolvedColumn(col_type=SchemaType("dict"), col_name="data", subkeys=["temp"])
    f = ExactFilter(col, "3.5")
    log_d(here, f.__class__.__name__)
    sql, params = f.to_sql(1)
    log_d(here, sql, params)
    f = ExactFilter(col, 3.5)
    log_d(here, f.__class__.__name__)
    sql, params = f.to_sql(1)
    log_d(here, sql, params)

    col = ResolvedColumn(col_type=SchemaType("list"), col_name="tags")
    f = HasFilter(col, ["admin", "editor"])
    sql, params = f.to_sql(1)
    log_d(here, sql, params)

    try:
        f = MinFilter(col, ["admin", "editor"])
        log_e(here, "Should raised ValueError!!!!")
    except ValueError as e:
        log_d(here, "raised ValueError on purpose:", e)

    log_d(here, SchemaType("time").normalize_value(1300000000))
