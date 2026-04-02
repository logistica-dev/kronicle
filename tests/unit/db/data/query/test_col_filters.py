# tests/unit/schemas/filters/test_col_filters.py
from datetime import datetime

from pytest import fixture, mark, raises

from kronicle.db.data.models.schema_types import SchemaType
from kronicle.db.data.query.col_filters import (
    AnyFilter,
    ExactFilter,
    HasFilter,
    MaxFilter,
    MinFilter,
    ResolvedColumn,
)
from kronicle.types.iso_datetime import IsoDateTime


# --------------------------------------------------------------------------------------------------
# Helper function to create ResolvedColumn
# --------------------------------------------------------------------------------------------------
def make_col(name="temperature", col_type="float", subkeys=None):
    return ResolvedColumn(col_name=name, col_type=SchemaType(col_type), subkeys=subkeys)


# --------------------------------------------------------------------------------------------------
# Fixtures for column types
# --------------------------------------------------------------------------------------------------
@fixture
def col_int():
    return ResolvedColumn(col_type=SchemaType("int"), col_name="count")


@fixture
def col_float():
    return ResolvedColumn(col_type=SchemaType("float"), col_name="temperature")


@fixture
def col_str():
    return ResolvedColumn(col_type=SchemaType("str"), col_name="name")


@fixture
def col_datetime():
    return ResolvedColumn(col_type=SchemaType("datetime"), col_name="time")


@fixture
def col_list():
    return ResolvedColumn(col_type=SchemaType("list"), col_name="tags")


@fixture
def col_dict_sub():
    return ResolvedColumn(col_type=SchemaType("dict"), col_name="data", subkeys=["temperature"])


# --------------------------------------------------------------------------------------------------
# ResolvedColumn validation
# --------------------------------------------------------------------------------------------------
def test_invalid_resolved_column():
    with raises(ValueError):
        ResolvedColumn(col_type=SchemaType("int"), col_name="author", subkeys=["age"])
    with raises(ValueError):
        ResolvedColumn(col_type=SchemaType("float"), col_name="author", subkeys=["height"])
    with raises(ValueError):
        ResolvedColumn(col_type=SchemaType("str"), col_name="author", subkeys=["name"])
    with raises(ValueError):
        ResolvedColumn(col_type=SchemaType("datetime"), col_name="data", subkeys=["time"])
    with raises(ValueError):
        ResolvedColumn(col_type=SchemaType("list"), col_name="data", subkeys=["time"])
    with raises(ValueError):
        ResolvedColumn(col_type=SchemaType("dict"), col_name="author")
    with raises(ValueError):
        ResolvedColumn(col_type=SchemaType("dict"), col_name="author.names", subkeys=["first_name"])
    with raises(ValueError):
        ResolvedColumn(col_type=SchemaType("str"), col_name="author.name")


# --------------------------------------------------------------------------------------------------
# Exact / col filter
# --------------------------------------------------------------------------------------------------
def test_exact_filter_top_level(col_int, col_float):
    for col in [col_int, col_float]:
        f = ExactFilter(col, 42)
        sql, params = f.to_sql(1)
        # Expect SQL cast for numeric/datetime
        if col.col_type.name in {"int", "float"}:
            assert sql.startswith(f"{col.col_name}")
        assert f.op == "="
        assert params[0] == 42


def test_exact_filter_sql():
    col = make_col()
    filt = ExactFilter(col, 42)
    sql, params = filt.to_sql(idx=1)
    assert sql == "temperature = $1"
    assert params == [42]


def testexact_filter_top_level(col_datetime):
    for date in [IsoDateTime(), datetime.now()]:
        f = ExactFilter(col_datetime, date)
        sql, params = f.to_sql(2)
        assert sql.startswith(f"{col_datetime.col_name}")
        assert f.op == "="
        assert params[0] == IsoDateTime(date)


def test_exact_filter_invalid(col_datetime, col_list):
    with raises(ValueError):
        ExactFilter(col_datetime, 42)
    with raises(ValueError):
        ExactFilter(col_list, 1)


def test_json_column_exact_filter():
    col = make_col(name="sensor", col_type="dict", subkeys=["temp"])
    filt = ExactFilter(col, 5.5)
    sql, params = filt.to_sql(idx=1)
    # LHS is casted for numeric, value kept as-is
    assert sql == "(sensor->>'temp') = $1"
    assert params[0] == "5.5"


# --------------------------------------------------------------------------------------------------
# Range filters: min/max (only numeric/datetime top-level)
# --------------------------------------------------------------------------------------------------
@mark.parametrize("FilterClass,expected_op", [(MinFilter, ">="), (MaxFilter, "<=")])
def test_range_filters_top_level(col_int, col_float, col_datetime, FilterClass, expected_op):
    for col in [col_int, col_float]:
        f = FilterClass(col, 42)
        sql, params = f.to_sql(1)
        # Expect SQL cast for numeric/datetime
        if col.col_type.name in {"int", "float"}:
            assert sql.startswith(f"{col.col_name}::numeric")
        assert f.op == expected_op
        assert params[0] == 42
    with raises(ValueError):
        FilterClass(col_datetime, 42)

    for date in [IsoDateTime(), datetime.now()]:
        f = FilterClass(col_datetime, date)
        sql, params = f.to_sql(2)
        assert sql.startswith(f"{col_datetime.col_name}::timestamptz")
        assert params[0] == IsoDateTime(date)


def test_range_filters_invalid(col_list, col_dict_sub):
    for col in [col_list, col_dict_sub]:
        for RangeFilter in [MinFilter, MaxFilter]:
            with raises(ValueError):
                RangeFilter(col, 1)


def test_min_filter_sql():
    col = make_col()
    filt = MinFilter(col, 10)
    sql, params = filt.to_sql(idx=1)
    assert sql == "temperature::numeric >= $1"
    assert params == [10]


def test_max_filter_sql():
    col = make_col()
    filt = MaxFilter(col, 100)
    sql, params = filt.to_sql(idx=1)
    assert sql == "temperature::numeric <= $1"
    assert params == [100]


def test_json_column_range_filter():
    col = make_col(name="sensor", col_type="dict", subkeys=["temp"])
    with raises(ValueError):
        MinFilter(col, 10)


# --------------------------------------------------------------------------------------------------
# Any filter
# --------------------------------------------------------------------------------------------------
@mark.parametrize("input_val,expected", [("1,2,3", [1, 2, 3]), ([4, 5], [4, 5])])
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
    with raises(ValueError):
        AnyFilter(col_list, ["1", "2"])
    # Invalid type
    with raises(ValueError):
        AnyFilter(col_int, "123.5")  # type: ignore # numeric input is valid only if wrapped in list


def test_any_filter_list_input():
    col = make_col()
    filt = AnyFilter(col, ["1", "2", "3"])
    sql, params = filt.to_sql(idx=1)
    assert sql == "temperature IN ($1,$2,$3)"
    assert params == [1, 2, 3]


def test_any_filter_csv_string_input():
    col = make_col("letter", "str")
    filt = AnyFilter(col, ["a", "b", "c"])
    sql, params = filt.to_sql(idx=1)
    assert sql == "letter IN ($1,$2,$3)"
    assert params == ["a", "b", "c"]


def test_any_filter_json_column():
    col = make_col(name="sensor", col_type="dict", subkeys=["temp"])
    filt = AnyFilter(col, ["10", "20"])
    sql, params = filt.to_sql(idx=1)
    assert sql == "(sensor->>'temp') IN ($1,$2)"
    assert params == ["10", "20"]


# --------------------------------------------------------------------------------------------------
# Has filter
# --------------------------------------------------------------------------------------------------
def test_has_filter_valid(col_list):
    f = HasFilter(col_list, ["admin", "editor"])
    sql, params = f.to_sql(1)
    assert params == ['["admin", "editor"]']
    assert sql == f"{col_list.col_name}::jsonb {HasFilter.op} $1::jsonb"


def test_has_filter_invalid(col_int):
    with raises(ValueError):
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


# --------------------------------------------------------------------------------------------------
# SQL generation for JSON paths
# --------------------------------------------------------------------------------------------------
def test_json_path_sql(col_dict_sub):
    assert col_dict_sub.json_path_sql == ("", "temperature")
    # Nested subkeys
    col_nested = ResolvedColumn(SchemaType("dict"), "dd", subkeys=["a", "b"])
    path, last = col_nested.json_path_sql
    assert path == "'a'"
    assert last == "b"
