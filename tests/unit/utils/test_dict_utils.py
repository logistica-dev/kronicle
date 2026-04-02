# tests/unit/utils/test_dict_utils.py

import pytest

from kronicle.utils import dict_utils

# --------------------------------------------------------------------------------------
# ensure_dict_or_none
# --------------------------------------------------------------------------------------


def test_ensure_dict_or_none_returns_empty_for_none():
    assert dict_utils.ensure_dict_or_none(None) == {}


def test_ensure_dict_or_none_returns_same_dict():
    d = {"a": 1, "b": 2}
    assert dict_utils.ensure_dict_or_none(d) == d


def test_ensure_dict_or_none_raises_type_error_for_non_dict():
    with pytest.raises(TypeError):
        dict_utils.ensure_dict_or_none([1, 2, 3])
    with pytest.raises(TypeError):
        dict_utils.ensure_dict_or_none("hello")
    # with field_name
    with pytest.raises(TypeError) as exc:
        dict_utils.ensure_dict_or_none("hello", field_name="myfield")
    assert "'myfield' must be a dict or None" in str(exc.value)


def test_ensure_dict_or_none_raises_value_error_for_empty_key():
    d = {"": 123}
    with pytest.raises(ValueError):
        dict_utils.ensure_dict_or_none(d)
    d2 = {" ": 123}
    with pytest.raises(ValueError) as exc:
        dict_utils.ensure_dict_or_none(d2, field_name="myfield")
    assert "Key cannot be empty for 'myfield'" in str(exc.value)


# --------------------------------------------------------------------------------------
# rows_to_columns
# --------------------------------------------------------------------------------------


def test_rows_to_columns_basic():
    rows = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    expected = {"a": [1, 3], "b": [2, 4]}
    result = dict_utils.rows_to_columns(rows)
    assert result == expected


def test_rows_to_columns_missing_keys():
    rows = [{"a": 1}, {"b": 2}]
    result = dict_utils.rows_to_columns(rows)
    assert result == {"a": [1], "b": [2]}


def test_rows_to_columns_empty_list():
    assert dict_utils.rows_to_columns([]) == {}


def test_rows_to_columns_non_overlapping_keys():
    rows = [{"x": 1}, {"y": 2}, {"z": 3}]
    result = dict_utils.rows_to_columns(rows)
    assert result == {"x": [1], "y": [2], "z": [3]}


# --------------------------------------------------------------------------------------
# strip_nulls
# --------------------------------------------------------------------------------------


def test_strip_nulls_dict_non_recursive():
    obj = {"a": 1, "b": None, "c": {"d": None, "e": 5}}
    result = dict_utils.strip_nulls(obj, recursive=False)
    assert result == {"a": 1, "c": {"d": None, "e": 5}}


def test_strip_nulls_dict_recursive():
    obj = {"a": 1, "b": None, "c": {"d": None, "e": 5}}
    result = dict_utils.strip_nulls(obj, recursive=True)
    assert result == {"a": 1, "c": {"e": 5}}


def test_strip_nulls_list_non_recursive():
    obj = [1, None, 2, None, 3]
    result = dict_utils.strip_nulls(obj, recursive=False)
    assert result == [1, 2, 3]


def test_strip_nulls_list_recursive():
    obj = [1, None, {"a": None, "b": 2}, [None, 3]]
    result = dict_utils.strip_nulls(obj, recursive=True)
    assert result == [1, {"b": 2}, [3]]


def test_strip_nulls_scalar_values():
    assert dict_utils.strip_nulls(5) == 5
    assert dict_utils.strip_nulls(None) is None
    assert dict_utils.strip_nulls("hello") == "hello"


def test_strip_nulls_empty_structures():
    assert dict_utils.strip_nulls({}) == {}
    assert dict_utils.strip_nulls([]) == []
