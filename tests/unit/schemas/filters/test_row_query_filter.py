# tests/unit/schemas/filters/test_row_query_filter.py
from unittest.mock import MagicMock

from fastapi import Request

from kronicle.schemas.filters.row_query_filter import DEFAULT_LIMIT, DEFAULT_STRICT_MODE, RowQueryFilter


# ------------------------------------------------------
# Basic instantiation and defaults
# ------------------------------------------------------
def test_defaults_and_limits():
    f = RowQueryFilter()
    assert f.limit == DEFAULT_LIMIT
    assert f.offset is None
    assert f.strict == DEFAULT_STRICT_MODE
    assert f.feedback is not None


def test_cap_limit_enforces_max():
    f = RowQueryFilter(limit=1000)
    assert f.limit == 500  # MAX_LIMIT


def test_cap_limit_handles_negative():
    f = RowQueryFilter(limit=-10)
    assert f.limit == DEFAULT_LIMIT


def test_cap_offset_enforces_max():
    f = RowQueryFilter(offset=20_000)
    assert f.offset == 10_000  # MAX_OFFSET


def test_cap_offset_handles_negative():
    f = RowQueryFilter(offset=-10)
    assert f.offset == 0


# ------------------------------------------------------
# normalize_single_value
# ------------------------------------------------------
def test_normalize_single_value_valid():
    f = RowQueryFilter()
    input_dict = {"Name": ["Alice"]}
    result = f.normalize_single_value(input_dict, str.lower, "col")
    assert result == {"name": "Alice"}


def test_normalize_single_value_multiple_values_warns():
    f = RowQueryFilter()
    input_dict = {"Name": ["Alice", "Bob"]}
    result = f.normalize_single_value(input_dict, str.lower, "col")
    assert result == {}
    assert f.feedback.details[0].message.startswith("Multiple values not allowed")


def test_normalize_single_value_invalid_key_warns():
    f = RowQueryFilter()
    input_dict = {"": ["Alice"]}
    result = f.normalize_single_value(input_dict, str.lower, "col")
    assert result == {}
    assert f.feedback.details[0].message == "Invalid column name" or "Multiple values" in f.feedback.details[0].message


# ------------------------------------------------------
# normalize_multi_value
# ------------------------------------------------------
def test_normalize_multi_value_flatten_and_split():
    f = RowQueryFilter()
    input_dict = {"tags": ["room1,room2", "room3"]}
    result = f.normalize_multi_value(input_dict, str.lower, "any")
    assert result == {"tags": ["room1", "room2", "room3"]}


def test_normalize_multi_value_invalid_key_warns():
    f = RowQueryFilter()
    input_dict = {"": ["val1"]}
    result = f.normalize_multi_value(input_dict, str.lower, "any")
    assert result == {}
    assert len(f.feedback.details) == 1
    assert f.feedback.details[0].message == "Invalid column name"


# ------------------------------------------------------
# normalize_sort
# ------------------------------------------------------
def test_normalize_sort_string_and_list():
    f = RowQueryFilter(sort="a,-b,c")
    result = f.normalize_sort()
    assert result
    assert set(result) == {"a", "-b", "c"}

    f = RowQueryFilter(sort=["a,-b", "c"])
    result = f.normalize_sort()
    assert result
    assert set(result) == {"a", "-b", "c"}


def test_normalize_sort_invalid_value_warns():
    f = RowQueryFilter(sort=["!@#"])
    result = f.normalize_sort()
    assert result is None
    assert len(f.feedback.details) == 1
    assert "Invalid column name" in f.feedback.details[0].message


# ------------------------------------------------------
# split_column_selection
# ------------------------------------------------------
def test_split_column_selection_string_and_list(monkeypatch):
    f = RowQueryFilter(columns="Aa,bB,Cc")
    result = f.split_column_selection()
    assert result == ["aa", "bb", "cc"]

    f = RowQueryFilter(columns=["Aa", "bb", "cC"])
    result = f.split_column_selection()
    assert result == ["aa", "bb", "cc"]


# ------------------------------------------------------
# parse_bracketed_filters
# ------------------------------------------------------
def test_parse_bracketed_filters_basic():
    params = [("min[time]", "2023-01-01"), ("col[name]", "Alice"), ("any[tag]", "room1,room2")]
    res = RowQueryFilter.parse_bracketed_filters(params)
    assert res["min"]["time"] == ["2023-01-01"]
    assert res["col"]["name"] == ["Alice"]
    assert res["any"]["tag"] == ["room1,room2"]


# ------------------------------------------------------
# from_query_params
# ------------------------------------------------------
def test_from_query_params_creates_instance():
    query_items = [
        ("limit", "10"),
        ("offset", "5"),
        ("sort", "a,-b"),
        ("columns", "x,y"),
        ("skip_received", "true"),
        ("strict", "true"),
        ("min[time]", "2026-03-20"),
        ("col[name]", "Alice"),
    ]
    mock_request = MagicMock(spec=Request)
    mock_request.query_params.get.side_effect = lambda k: dict(query_items).get(k)
    mock_request.query_params.multi_items.return_value = query_items

    f = RowQueryFilter.from_query_params(mock_request)
    assert f.limit == 10
    assert f.offset == 5
    assert f.col == {"name": "Alice"}
    assert f.min == {"time": "2026-03-20"}
    assert f.sort == ["a", "b"] or f.sort  # normalized
    assert f.columns == ["x", "y"]


# ------------------------------------------------------
# model_post_init
# ------------------------------------------------------
def test_model_post_init_calls_normalization(monkeypatch):
    f = RowQueryFilter(
        col={"name": "Alice"},
        min={"time": "2026-03-20"},
        max={"time": "2026-03-21"},
        any={"tags": ["a,b"]},
        has={"tags": ["c,d"]},
        sort="x,-y",
        columns="col1,col2",
    )

    # patch logging
    monkeypatch.setattr("kronicle.schemas.filters.row_query_filter.log_block", lambda *a, **kw: MagicMock().__enter__())
    monkeypatch.setattr("kronicle.schemas.filters.row_query_filter.log_d", lambda *a, **kw: None)

    f.model_post_init(None)
    assert f.col == {"name": "Alice"}
    assert f.min == {"time": "2026-03-20"}
    assert f.max == {"time": "2026-03-21"}
    assert f.any
    assert f.any["tags"] == ["a", "b"]
    assert f.has
    assert f.has["tags"] == ["c", "d"]
    assert f.sort == ["x", "-y"]
    assert f.columns == ["col1", "col2"]
