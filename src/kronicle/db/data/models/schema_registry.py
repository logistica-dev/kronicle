# kronicle/db/data/models/schema_registry.py

from typing import Any, Type
from uuid import UUID

from kronicle.types.iso_datetime import IsoDateTime
from kronicle.utils.num_utils import normalize_float, normalize_int


class SchemaTypeInfo:
    def __init__(self, canonical: str, db_type: str, py_type: Type, aliases: list[str]):
        self.canonical = canonical
        self.db_type = db_type
        self.py_type = py_type
        self.aliases = aliases

    def validate(self, value: Any) -> Any:
        """Validate and normalize value according to type."""
        if value is None:
            return None
        if self.canonical == "datetime":
            return IsoDateTime.normalize_value(value)
        if self.canonical == "float" and isinstance(value, (int, float)):
            return float(value)
        if not isinstance(value, self.py_type):
            raise TypeError(f"Expected {self.canonical}, got {type(value).__name__}")
        return value

    @staticmethod
    def _normalize_float(value):
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError as e:
                raise ValueError(f"Cannot normalize type '{value}' to float") from e
        raise ValueError(f"Cannot normalize type '{type(value).__name__}' to float")

    def normalize_value(self, value: Any) -> Any:
        """
        Normalize a value to the canonical Python type for this schema type.
        Handles datetime, JSON, and numeric conversions.
        """
        if self.canonical == "datetime":
            return IsoDateTime.normalize_value(value)

        if self.canonical in {"dict", "list"}:
            if isinstance(value, (dict, list)):
                return value
            raise ValueError(f"Cannot normalize type '{type(value).__name__}' to {self.canonical}")

        if self.canonical == "float":
            return normalize_float(value)

        if self.canonical == "int":
            return normalize_int(value)

        expected = self.py_type
        if isinstance(value, expected):
            return value
        raise ValueError(f"Expected type {self.canonical}, got {type(value).__name__}")

    def __str__(self) -> str:
        return (
            f"SchemaTypeInfo(can={self.canonical},db={self.db_type},py={self.py_type.__name__},aliases={self.aliases})"
        )


class SchemaRegistry:
    """
    Singleton registry of all allowed SchemaTypes.
    Provides lookup, validation, and DB/Python mappings.
    """

    _instance: "SchemaRegistry|None" = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_registry()
        return cls._instance

    def _init_registry(self):
        """Initialize the type registry."""
        self._types = [
            SchemaTypeInfo("str", "TEXT", str, ["string", "str", "text"]),
            SchemaTypeInfo("int", "INTEGER", int, ["int", "integer"]),
            SchemaTypeInfo("float", "DOUBLE PRECISION", float, ["float", "number", "double", "double precision"]),
            SchemaTypeInfo("bool", "BOOLEAN", bool, ["bool", "boolean"]),
            SchemaTypeInfo("uuid", "UUID", UUID, ["uuid", "uuid4", "uuidv4", "uuid_v4"]),
            SchemaTypeInfo(
                "datetime", "TIMESTAMPTZ", IsoDateTime, ["datetime", "date", "timestamp", "timestamptz", "time"]
            ),
            SchemaTypeInfo("dict", "JSONB", dict, ["dict", "json", "jsonb"]),
            SchemaTypeInfo("list", "JSONB", list, ["list"]),
        ]
        # Build lookup maps
        self._alias_map = {alias.lower(): t for t in self._types for alias in t.aliases}
        self._canonical_map = {t.canonical: t for t in self._types}
        self._allowed_types = list(self._canonical_map.keys())

    # -----------------------------
    # Public methods
    # -----------------------------
    @property
    def allowed_types(self) -> list[str]:
        return self._allowed_types

    def get_by_alias(self, alias: str) -> SchemaTypeInfo:
        """Return SchemaTypeInfo from any alias."""
        typ = self._alias_map.get(alias.lower())
        if not typ:
            raise ValueError(f"Unknown type alias: '{alias}'")
        return typ

    def get_by_canonical(self, canonical: str) -> SchemaTypeInfo:
        """Return SchemaTypeInfo from canonical name."""
        typ = self._canonical_map.get(canonical)
        if not typ:
            raise ValueError(f"Unknown canonical type: '{canonical}'")
        return typ

    def user_to_canonical(self, user_type: str) -> str:
        """Map user-provided type to canonical type."""
        return self.get_by_alias(user_type).canonical

    def canonical_to_db(self, canonical: str) -> str:
        """Get the DB type for a canonical type."""
        return self.get_by_canonical(canonical).db_type

    def canonical_to_py(self, canonical: str) -> Type:
        """Get the Python type for a canonical type."""
        return self.get_by_canonical(canonical).py_type

    def validate_value(self, value: Any, type_name: str) -> Any:
        """Validate a value according to canonical or alias type."""
        typ = self._alias_map.get(type_name.lower()) or self._canonical_map.get(type_name)
        if not typ:
            raise ValueError(f"Unknown type for validation: '{type_name}'")
        return typ.validate(value)

    def normalize_value(self, value: Any, canonical: str) -> Any:
        """
        Normalize a value using the registered SchemaType.
        """
        type_info = self.get_by_canonical(canonical)  # lookup SchemaTypeInfo
        return type_info.normalize_value(value)


if __name__ == "__main__":  # pragma: no cover
    print(SchemaRegistry().get_by_alias("TIMESTAMPTZ").canonical)
    print(SchemaTypeInfo("uuid", "UUID", UUID, ["uuid", "uuid4", "uuidv4", "uuid_v4"]))
    print(SchemaTypeInfo("str", "TEXT", str, ["string", "str", "text"]))
