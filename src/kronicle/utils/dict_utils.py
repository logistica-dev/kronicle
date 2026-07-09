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


def sanitize_dict(
    d: Any,
    max_depth: int = 5,
    max_keys: int = 100,
    max_string_len: int = 1000,
    current_depth: int = 0,
) -> Any:
    """
    Recursively sanitize a dictionary to prevent DB pollution and DoS attacks.
    """
    if current_depth > max_depth:
        raise ValueError("Max depth exceeded")

    if isinstance(d, dict):
        if len(d) > max_keys:
            raise ValueError("Too many keys in dictionary")

        sanitized = {}
        for k, v in d.items():
            if not isinstance(k, str):
                raise TypeError(f"Key must be a string, got {type(k)}")
            if len(k) > max_string_len:
                raise ValueError("Key string too long")
            sanitized[k] = sanitize_dict(v, max_depth, max_keys, max_string_len, current_depth + 1)
        return sanitized

    elif isinstance(d, list):
        if len(d) > max_keys:
            raise ValueError("List too long")
        return [sanitize_dict(v, max_depth, max_keys, max_string_len, current_depth + 1) for v in d]

    elif isinstance(d, str):
        if len(d) > max_string_len:
            raise ValueError("String too long")
        return d

    elif isinstance(d, (int, float, bool)) or d is None:
        return d

    else:
        raise TypeError(f"Unsupported type: {type(d)}")


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
