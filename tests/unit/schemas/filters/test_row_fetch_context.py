# test_filters.py

from kronicle.db.data.models.schema_types import SchemaType
from kronicle.schemas.filters.row_fetch_context import (
    AnyFilter,
    ExactFilter,
    MaxFilter,
    MinFilter,
    ResolvedColumn,
)


# -------------------------------
# Helper function to create ResolvedColumn
# -------------------------------
def make_col(name="temperature", col_type="float", subkeys=None):
    return ResolvedColumn(col_name=name, col_type=SchemaType(col_type), subkeys=subkeys or [])


# -------------------------------
# ExactFilter tests
# -------------------------------
def test_exact_filter_sql():
    col = make_col()
    filt = ExactFilter(col, 42)
    sql, params = filt.to_sql(idx=1)
    assert sql == "temperature = $1"
    assert params == [42]


# -------------------------------
# MinFilter tests
# -------------------------------
def test_min_filter_sql():
    col = make_col()
    filt = MinFilter(col, 10)
    sql, params = filt.to_sql(idx=1)
    assert sql == "temperature >= $1::numeric"
    assert params == [10]


# -------------------------------
# MaxFilter tests
# -------------------------------
def test_max_filter_sql():
    col = make_col()
    filt = MaxFilter(col, 100)
    sql, params = filt.to_sql(idx=1)
    assert sql == "temperature <= $1::numeric"
    assert params == [100]


# -------------------------------
# JSON/dict column tests
# -------------------------------
def test_json_column_exact_filter():
    col = make_col(name="sensor", col_type="float", subkeys=["temp"])
    filt = ExactFilter(col, 5.5)
    sql, params = filt.to_sql(idx=1)
    # LHS is casted for numeric, value kept as-is
    assert sql == "(sensor->>'temp') = $1"
    assert params == [5.5]


def test_json_column_range_filter():
    col = make_col(name="sensor", col_type="float", subkeys=["temp"])
    filt = MinFilter(col, 10)
    sql, params = filt.to_sql(idx=1)
    assert sql == "(sensor->>'temp')::numeric >= $1"
    assert params == [10]


# -------------------------------
# AnyFilter tests
# -------------------------------
def test_any_filter_list_input():
    col = make_col()
    filt = AnyFilter(col, [1, 2, 3])
    sql, params = filt.to_sql(idx=1)
    assert sql == "temperature IN ($1,$2,$3)"
    assert params == [1, 2, 3]


def test_any_filter_csv_string_input():
    col = make_col()
    filt = AnyFilter(col, "a,b, c")
    sql, params = filt.to_sql(idx=1)
    assert sql == "temperature IN ($1,$2,$3)"
    assert params == ["a", "b", "c"]


def test_any_filter_json_column():
    col = make_col(name="sensor", col_type="float", subkeys=["temp"])
    filt = AnyFilter(col, [10, 20])
    sql, params = filt.to_sql(idx=1)
    assert sql == "(sensor->>'temp') IN ($1,$2)"
    assert params == [10, 20]
