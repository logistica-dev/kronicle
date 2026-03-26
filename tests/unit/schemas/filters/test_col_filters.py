# tests/unit/schemas/filters/test_col_filters.py
from datetime import datetime

import pytest

from kronicle.db.data.models.schema_types import SchemaType
from kronicle.schemas.filters.col_filters import (
    AnyFilter,
    ExactFilter,
    HasFilter,
    MaxFilter,
    MinFilter,
    ResolvedColumn,
)
from kronicle.types.iso_datetime import IsoDateTime


# ----------------------------------------------------------------------------------------------
# Fixtures for column types
# ----------------------------------------------------------------------------------------------
@pytest.fixture
def col_int():
    return ResolvedColumn(col_type=SchemaType("int"), col_name="count")


@pytest.fixture
def col_float():
    return ResolvedColumn(col_type=SchemaType("float"), col_name="temperature")


@pytest.fixture
def col_str():
    return ResolvedColumn(col_type=SchemaType("str"), col_name="name")


@pytest.fixture
def col_datetime():
    return ResolvedColumn(col_type=SchemaType("datetime"), col_name="time")


@pytest.fixture
def col_list():
    return ResolvedColumn(col_type=SchemaType("list"), col_name="tags")


@pytest.fixture
def col_dict_sub():
    return ResolvedColumn(col_type=SchemaType("dict"), col_name="data", subkeys=["temperature"])


# ----------------------------------------------------------------------------------------------
# ResolvedColumn validation
# ----------------------------------------------------------------------------------------------
def test_invalid_resolved_column():
    with pytest.raises(ValueError):
        ResolvedColumn(col_type=SchemaType("int"), col_name="author", subkeys=["age"])
    with pytest.raises(ValueError):
        ResolvedColumn(col_type=SchemaType("float"), col_name="author", subkeys=["height"])
    with pytest.raises(ValueError):
        ResolvedColumn(col_type=SchemaType("str"), col_name="author", subkeys=["name"])
    with pytest.raises(ValueError):
        ResolvedColumn(col_type=SchemaType("datetime"), col_name="data", subkeys=["time"])
    with pytest.raises(ValueError):
        ResolvedColumn(col_type=SchemaType("list"), col_name="data", subkeys=["time"])
    with pytest.raises(ValueError):
        ResolvedColumn(col_type=SchemaType("dict"), col_name="author")
    with pytest.raises(ValueError):
        ResolvedColumn(col_type=SchemaType("dict"), col_name="author.names", subkeys=["first_name"])
    with pytest.raises(ValueError):
        ResolvedColumn(col_type=SchemaType("str"), col_name="author.name")


# ----------------------------------------------------------------------------------------------
# Exact / col filter
# ----------------------------------------------------------------------------------------------
def test_exact_filter_top_level(col_int, col_float):
    for col in [col_int, col_float]:
        f = ExactFilter(col, 42)
        sql, params = f.to_sql(1)
        # Expect SQL cast for numeric/datetime
        if col.col_type.name in {"int", "float"}:
            assert sql.startswith(f"{col.col_name}")
        assert f.op == "="
        assert params[0] == 42


def testexact_filter_top_level(col_datetime):
    for date in [IsoDateTime(), datetime.now()]:
        f = ExactFilter(col_datetime, date)
        sql, params = f.to_sql(2)
        assert sql.startswith(f"{col_datetime.col_name}")
        assert f.op == "="
        assert params[0] == IsoDateTime(date)


def test_exact_filter_invalid(col_datetime, col_list):
    with pytest.raises(ValueError):
        ExactFilter(col_datetime, 42)
    with pytest.raises(ValueError):
        ExactFilter(col_list, 1)


# ----------------------------------------------------------------------------------------------
# Range filters: min/max (only numeric/datetime top-level)
# ----------------------------------------------------------------------------------------------
@pytest.mark.parametrize("FilterClass,expected_op", [(MinFilter, ">="), (MaxFilter, "<=")])
def test_range_filters_top_level(col_int, col_float, col_datetime, FilterClass, expected_op):
    for col in [col_int, col_float]:
        f = FilterClass(col, 42)
        sql, params = f.to_sql(1)
        # Expect SQL cast for numeric/datetime
        if col.col_type.name in {"int", "float"}:
            assert sql.startswith(f"{col.col_name}::numeric")
        assert f.op == expected_op
        assert params[0] == 42
    with pytest.raises(ValueError):
        FilterClass(col_datetime, 42)

    for date in [IsoDateTime(), datetime.now()]:
        f = FilterClass(col_datetime, date)
        sql, params = f.to_sql(2)
        assert sql.startswith(f"{col_datetime.col_name}::timestamptz")
        assert params[0] == IsoDateTime(date)


def test_range_filters_invalid(col_list, col_dict_sub):
    for col in [col_list, col_dict_sub]:
        for RangeFilter in [MinFilter, MaxFilter]:
            with pytest.raises(ValueError):
                RangeFilter(col, 1)


# ----------------------------------------------------------------------------------------------
# Any filter
# ----------------------------------------------------------------------------------------------
@pytest.mark.parametrize("input_val,expected", [("1,2,3", [1, 2, 3]), ([4, 5], [4, 5])])
def test_any_filter_scalars(col_int, input_val, expected):
    f = AnyFilter(col_int, input_val)
    sql, params = f.to_sql(1)
    assert params == expected
    assert f.op == "IN"
    assert sql.startswith("count")


def test_any_filter_dict_subkeys(col_dict_sub):
    f = AnyFilter(col_dict_sub, ["a", "b", "c"])
    sql, params = f.to_sql(1)
    assert params == ["a", "b", "c"]


def test_any_filter_invalid(col_list, col_int):
    # Not allowed on list
    with pytest.raises(ValueError):
        AnyFilter(col_list, ["1", "2"])
    # Invalid type
    with pytest.raises(ValueError):
        AnyFilter(col_int, "123.5")  # type: ignore # numeric input is valid only if wrapped in list


# ----------------------------------------------------------------------------------------------
# Has filter
# ----------------------------------------------------------------------------------------------
def test_has_filter_valid(col_list):
    f = HasFilter(col_list, ["admin", "editor"])
    sql, params = f.to_sql(1)
    assert params == ['["admin", "editor"]']
    assert sql == f"{col_list.col_name}::jsonb {HasFilter.op} $1::jsonb"


def test_has_filter_invalid(col_int):
    with pytest.raises(ValueError):
        HasFilter(col_int, ["a"])


def test_has_filter_string_input(col_list):
    f = HasFilter(col_list, "admin,editor")
    sql, params = f.to_sql(1)
    assert params == ['["admin", "editor"]']
    assert sql == f"{col_list.col_name}::jsonb {HasFilter.op} $1::jsonb"


def test_has_filter_single_value(col_list):
    f = HasFilter(col_list, "admin")
    sql, params = f.to_sql(1)
    assert params == ['["admin"]']
    assert sql == f"{col_list.col_name}::jsonb {HasFilter.op} $1::jsonb"


def test_has_filter_one_element_list(col_list):
    f = HasFilter(col_list, ["admin"])
    sql, params = f.to_sql(1)
    assert params == ['["admin"]']
    assert sql == f"{col_list.col_name}::jsonb {HasFilter.op} $1::jsonb"


# ----------------------------------------------------------------------------------------------
# SQL generation for JSON paths
# ----------------------------------------------------------------------------------------------
def test_json_path_sql(col_dict_sub):
    assert col_dict_sub.json_path_sql == ("", "temperature")
    # Nested subkeys
    col_nested = ResolvedColumn(SchemaType("dict"), "dd", subkeys=["a", "b"])
    path, last = col_nested.json_path_sql
    assert path == "'a'"
    assert last == "b"
