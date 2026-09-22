# kronicle/utils/dict_utils.py
from collections import defaultdict
from typing import Any


def ensure_dict_or_none(d, field_name: str | None = None):
    """Ensure a field is a dict with non-empty keys."""
    if d is None:
        return {}
    if not isinstance(d, dict):
        if field_name:
            raise TypeError(f"'{field_name}' must be a dict or None")
        raise TypeError("Must be a dict or None")
    for key in d.keys():
        if not key.strip():
            if field_name:
                raise ValueError(f"Key cannot be empty for '{field_name}'")
            raise ValueError("Key cannot be empty")
    return d


def rows_to_columns(rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    """
    Convert row-oriented data into column-oriented form.
    Example:
        [{"a":1,"b":2}, {"a":3,"b":4}] → {"a":[1,3], "b":[2,4]}
    """
    cols = defaultdict(list)
    for row in rows:
        for k, v in row.items():
            cols[k].append(v)
    return dict(cols)


def strip_nulls(obj, recursive: bool = False):
    """
    Removes the None values
    """
    if isinstance(obj, dict):
        return {k: strip_nulls(v, recursive) if recursive else v for k, v in obj.items() if v is not None}
    elif isinstance(obj, list):
        return [strip_nulls(v, recursive) for v in obj if v is not None]
    return obj


def remove_alt_field(d: dict, keep: str, alt: str):
    if alt in d:
        d.setdefault(keep, d.pop(alt))


_SCALAR_TYPES = (int, float, bool)


def validate_dict(
    d: Any,
    max_depth: int = 5,
    max_keys: int = 100,
    max_string_len: int = 1000,
    current_depth: int = 0,
) -> Any:
    """
    Recursively validate a JSON-like structure to prevent DB pollution and DoS attacks.

    Checks depth, collection size, key types/lengths, and supported value types.
    Returns a copy of the input unchanged when valid.
    """
    if current_depth > max_depth:
        raise ValueError("Max depth exceeded")

    if isinstance(d, dict):
        return _validate_mapping(d, max_depth, max_keys, max_string_len, current_depth)
    if isinstance(d, list):
        return _validate_sequence(d, max_depth, max_keys, max_string_len, current_depth)
    return _validate_scalar(d, max_string_len)


def _validate_mapping(
    d: dict,
    max_depth: int,
    max_keys: int,
    max_string_len: int,
    current_depth: int,
) -> dict:
    if len(d) > max_keys:
        raise ValueError("Too many keys in dictionary")
    next_depth = current_depth + 1
    return {
        _validate_key(k, max_string_len): validate_dict(v, max_depth, max_keys, max_string_len, next_depth)
        for k, v in d.items()
    }


def _validate_key(k: Any, max_string_len: int) -> str:
    if not isinstance(k, str):
        raise TypeError(f"Key must be a string, got {type(k)}")
    if len(k) > max_string_len:
        raise ValueError("Key string too long")
    return k


def _validate_sequence(seq: list, max_depth: int, max_keys: int, max_string_len: int, current_depth: int) -> list:
    if len(seq) > max_keys:
        raise ValueError("List too long")
    next_depth = current_depth + 1
    return [validate_dict(v, max_depth, max_keys, max_string_len, next_depth) for v in seq]


def _validate_scalar(v: Any, max_string_len: int) -> Any:
    if isinstance(v, str):
        if len(v) > max_string_len:
            raise ValueError("String too long")
        return v
    if v is None or isinstance(v, _SCALAR_TYPES):
        return v
    raise TypeError(f"Unsupported type: {type(v)}")


if __name__ == "__main__":  # pragma: no cover
    here = "dict_utils.tests"
    print(here, "strip_nulls list:", strip_nulls([3, 0, 5, None]))
    print(
        here,
        "strip_nulls dict:",
        strip_nulls({"a": 3, "b": 0, "5": "zeruiogh", "d": None, "e": {"g": None, "h": "testsingt"}}, True),
    )
    print(here, "strip_nulls tutu:", strip_nulls("tutu"))
    print(here, "strip_nulls None:", strip_nulls(None))
