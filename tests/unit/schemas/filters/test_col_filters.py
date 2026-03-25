# tests/test_filters.py
from datetime import datetime

import pytest

from kronicle.db.data.models.schema_types import SchemaType
from kronicle.schemas.filters.col_filters import (
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
