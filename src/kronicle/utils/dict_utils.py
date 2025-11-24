from collections import defaultdict
from typing import Any


def ensure_dict_or_none(d, field_name: str | None = None):
    """Ensure a field is a dict with non-empty keys."""
    if d is None:
        return {}
    if not isinstance(d, dict):
        if field_name:
            raise TypeError(f"{field_name} must be a dict or None")
        raise TypeError("Must be a dict or None")
    for key in d.keys():
        if not key.strip():
            if field_name:
                raise ValueError(f"Key cannot be empty for {field_name}")
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


if __name__ == "__main__":
    here = "dict_utils.tests"
    print(here, "strip_nulls list:", strip_nulls([3, 0, 5, None]))
    print(
        here,
        "strip_nulls dict:",
        strip_nulls({"a": 3, "b": 0, "5": "zeruiogh", "d": None, "e": {"g": None, "h": "totot"}}, True),
    )
    print(here, "strip_nulls tutu:", strip_nulls("tutu"))
    print(here, "strip_nulls None:", strip_nulls(None))
