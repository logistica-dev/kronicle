# kronicle/types/schema_types.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from kronicle.types.errors import BadRequestError
from kronicle.types.iso_datetime import IsoDateTime

# --------------------------------------------------------------------------------------------------
# UserType -> SchemaType -> DBType mappings
# --------------------------------------------------------------------------------------------------
# User-friendly aliases (case-insensitive)
USER_TYPE_MAP = {
    "string": "str",
    "str": "str",
    "text": "str",
    "number": "float",
    "double": "float",
    "double precision": "float",
    "float": "float",
    "integer": "int",
    "int": "int",
    "boolean": "bool",
    "bool": "bool",
    "datetime": "datetime",
    "date": "datetime",
    "timestamp": "datetime",
    "timestampz": "datetime",
    "time": "datetime",
    "json": "dict",
    "jsonb": "dict",
    "dict": "dict",
    "list": "list",
}

# SchemaType -> SQL DB types
COL_TO_DB_TYPE = {
    "str": "TEXT",
    "int": "INTEGER",
    "float": "DOUBLE PRECISION",
    "bool": "BOOLEAN",
    "datetime": "TIMESTAMPTZ",
    "dict": "JSONB",
    "list": "JSONB",
}

# Python type objects for runtime validation
COL_TO_PY_TYPE = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "datetime": IsoDateTime,  # use custom subclass
    "dict": dict,
    "list": list,
}


# --------------------------------------------------------------------------------------------------
# SchemaType class
# --------------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class SchemaType:
    """
    Represents a canonical column type.
    Converts from user input, validates values, and maps to DB types.
    Immutable and hashable for safe dict/set usage.
    Supports optional fields: 'optional[str]', 'optional[int]', etc.
    """

    name: str
    optional: bool = False  # True if nullable

    def __post_init__(self):
        normalized = str(self.name).lower()
        if normalized not in COL_TO_PY_TYPE:
            raise ValueError(
                f"Developer error: Unknown SchemaType '{self.name}'. " f"Expected one of {self.get_py_values()}"
            )
        object.__setattr__(self, "sensor_name", normalized)

    # ----------------------------------------------------------------------------------------------
    # Construction helpers
    # ----------------------------------------------------------------------------------------------
    @classmethod
    def get_py_values(cls) -> list[str]:
        return list(COL_TO_PY_TYPE.keys())

    @classmethod
    def from_user_type(cls, user_type: str) -> "SchemaType":
        normalized = str(user_type).lower()
        optional = False

        if normalized.startswith("optional[") and normalized.endswith("]"):
            inner = normalized[len("optional[") : -1].strip()
            normalized = inner
            optional = True

        if normalized not in USER_TYPE_MAP:
            raise BadRequestError(
                f"Unsupported user type: '{user_type}', " f"expected one of {list(USER_TYPE_MAP.keys())}"
            )
        return cls(name=USER_TYPE_MAP[normalized], optional=optional)

    # ----------------------------------------------------------------------------------------------
    # Validation & mapping
    # ----------------------------------------------------------------------------------------------
    @property
    def db_type(self) -> str:
        try:
            return COL_TO_DB_TYPE[self.name]
        except KeyError as e:
            raise TypeError(f"Developer error: SchemaType '{self.name}' not in {self.get_py_values()}") from e

    @property
    def py_type(self):
        """Return the underlying Python type object (e.g. str, int, dict)."""
        return COL_TO_PY_TYPE[self.name]

    def is_json(self) -> bool:
        """True if the type maps to JSONB in DB (dict or list)."""
        return self.name in {"dict", "list"}

    def validate(self, value: Any, col_name: str = "") -> Any:
        """
        Validate a runtime value against this SchemaType.
        - Accepts ISO strings for datetime
        - Raises BadRequestError if invalid user value
        - Raises TypeError if developer misuse (wrong SchemaType class setup)
        """
        if value is None:
            if self.optional:
                return None
            raise BadRequestError(f"Expected value of type '{self.name}' for column '{col_name}', got None")

        try:
            expected = self.py_type
        except KeyError as e:
            raise TypeError(f"Developer error: SchemaType '{self.name}' not in {self.get_py_values()}") from e
        col_display = f" for column '{col_name}'" if col_name else ""
        if self.name == "datetime":
            try:
                return IsoDateTime.normalize_value(value)
            except Exception as e:
                raise BadRequestError(f"Expected datetime or ISO string{col_display}, got '{value}'") from e

        if expected is float and isinstance(value, (float, int)):
            return float(value)

        if not isinstance(value, expected):
            raise BadRequestError(f"Expected value of type '{self.name}'{col_display}, got '{type(value).__name__}'")
        return value

    # inside SchemaType
    def normalize_value(self, value: Any) -> Any:
        """
        Normalize a value to the canonical Python type:
        - str, int, float, bool, dict, list -> unchanged
        - datetime -> use IsoDateTime.normalize_value
        """
        if self.name == "datetime":
            from kronicle.types.iso_datetime import IsoDateTime

            return IsoDateTime.normalize_value(value)

        elif self.name in {"dict", "list"}:
            if isinstance(value, (dict, list)):
                return value
            raise BadRequestError(f"Cannot normalize type '{type(value).__name__}' to {self.name}")

        else:
            expected = self.py_type
            if not isinstance(value, expected):
                raise BadRequestError(f"Expected type {self.name}, got {type(value).__name__}")
            return value

    # ----------------------------------------------------------------------------------------------
    # Representation & comparison
    # ----------------------------------------------------------------------------------------------
    def __eq__(self, other):
        if isinstance(other, SchemaType):
            return self.name == other.name
        if isinstance(other, str):
            return self.name == other.lower()
        return False

    def __repr__(self):
        return f"SchemaType({self.name!r})"

    def __str__(self):
        return self.name


# --------------------------------------------------------------------------------------------------
# Main test / sanity check
# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    from kronicle.utils.dev_logs import log_d

    here = "schema_types.test"

    log_d(here, "=== SchemaType tests ===")

    # -- from user types --
    t_str = SchemaType.from_user_type("string")
    t_float = SchemaType.from_user_type("float")
    t_list = SchemaType.from_user_type("list")
    t_time = SchemaType.from_user_type("time")
    t_bool = SchemaType.from_user_type("bool")
    t_json = SchemaType.from_user_type("json")
    t_int = SchemaType.from_user_type("integer")

    log_d(here, "t_str", t_str, "-> DB:", t_str.db_type, "-> py:", t_str.py_type)
    log_d(here, "t_float", t_float, "-> DB:", t_float.db_type, "-> py:", t_float.py_type)
    log_d(
        here,
        "t_list",
        t_list,
        "-> DB:",
        t_list.db_type,
        "-> py:",
        t_list.py_type,
        "is_json:",
        t_list.is_json(),
    )
    log_d(
        here,
        "t_time",
        t_time,
        "-> DB:",
        t_time.db_type,
        "-> py:",
        t_time.py_type,
        "is_json:",
        t_time.is_json(),
    )
    log_d(here, "t_bool", t_bool, "-> DB:", t_bool.db_type, "-> py:", t_bool.py_type)
    log_d(
        here,
        "t_json",
        t_json,
        "-> DB:",
        t_json.db_type,
        "-> py:",
        t_json.py_type,
        "is_json:",
        t_json.is_json(),
    )
    log_d(here, "t_int", t_int, "-> DB:", t_int.db_type, "-> py:", t_int.py_type)

    # -- equality checks --
    assert t_str == "str"
    assert t_float == SchemaType("float")
    assert t_list != "dict"
    assert t_time == "datetime"

    # -- value validation --
    try:
        t_str.validate("hello")  # ok
        log_d(here, "Validation OK (string)")
    except Exception as e:
        log_d(here, "Unexpected error:", e)

    try:
        t_float.validate("not a float")
    except BadRequestError as e:
        log_d(here, "Caught expected BadRequestError :", e)

    # -- datetime normalization --
    dt_schema = SchemaType("datetime")
    iso_value = dt_schema.normalize_value("2025-09-17T22:30:00+02:00")
    log_d(here, "Normalized datetime :", iso_value, type(iso_value))

    # -- unknown type --
    try:
        SchemaType("nonexistent")
    except ValueError as e:
        log_d(here, "Caught expected ValueError :", e)

    log_d(here, "=== End of SchemaType tests ===")
