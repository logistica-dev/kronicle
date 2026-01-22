# kronicle/db/data/models/schema_types.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Type

from kronicle.db.data.models.schema_registry import SchemaRegistry
from kronicle.utils.dev_logs import log_i

mod = "sch_typ"


# --------------------------------------------------------------------------------------------------
# SchemaType class
# --------------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class SchemaType:
    """
    Immutable, hashable type descriptor for channel columns.

    Responsibilities:
    - Stores canonical type name and optional flag.
    - Delegates validation and type mapping to SchemaRegistry.
    - Supports optional types via 'optional[...]' notation.
    """

    name: str
    optional: bool = False

    def __post_init__(self):
        # Normalize optional[...] syntax
        n = str(self.name).strip().lower()
        opt = self.optional
        if n.startswith("optional[") and n.endswith("]"):
            n = n[len("optional[") : -1].strip()
            opt = True

        # Validate canonical type via registry
        registry = SchemaRegistry()
        try:
            c = registry.user_to_canonical(n)
        except ValueError as e:
            raise ValueError(f"Unsupported type '{self.name}'. Allowed types: {list(registry.allowed_types)}") from e

        # Set normalized attributes
        object.__setattr__(self, "name", c)
        object.__setattr__(self, "optional", opt)

    # ----------------------------------------------------------------------------------------------
    # Type mapping
    # ----------------------------------------------------------------------------------------------
    @property
    def db_type(self) -> str:
        """Return the SQL DB type corresponding to this canonical type."""
        return SchemaRegistry().canonical_to_db(self.name)

    @property
    def py_type(self) -> Type:
        """Return the Python type corresponding to this canonical type."""
        return SchemaRegistry().canonical_to_py(self.name)

    @classmethod
    def from_str(cls, user_type: str) -> SchemaType:
        return SchemaType(user_type)

    # ----------------------------------------------------------------------------------------------
    # Validation
    # ----------------------------------------------------------------------------------------------
    def validate(self, value: Any) -> Any:
        """
        Validate a value against this SchemaType.
        Accepts ISO strings for datetime.
        Returns normalized value.
        """
        if value is None:
            if self.optional:
                return None
            raise ValueError(f"Value for type '{self.name}' cannot be None")
        try:
            valid = SchemaRegistry().validate_value(value, self.name)
        except (ValueError, TypeError) as e:
            raise ValueError(str(e)) from e
        return valid

    # ----------------------------------------------------------------------------------------------
    # Normalization
    # ----------------------------------------------------------------------------------------------
    def normalize_value(self, value: Any) -> Any:
        """
        Normalize a value to the canonical Python type for this SchemaType.
        - str, int, float, bool, dict, list -> unchanged
        - datetime -> IsoDateTime
        - Raises ValueError if value cannot be converted
        """
        if value is None:
            if self.optional:
                return None
            raise ValueError(f"Cannot normalize None for type '{self.name}'")
        return SchemaRegistry().normalize_value(value, self.name)

    # ----------------------------------------------------------------------------------------------
    # Utility
    # ----------------------------------------------------------------------------------------------
    def is_json(self) -> bool:
        """True if this type maps to JSONB in DB (dict or list)."""
        return self.name in {"dict", "list"}

    # ----------------------------------------------------------------------------------------------
    # Representation & comparison
    # ----------------------------------------------------------------------------------------------
    def __repr__(self):
        print(f"SchemaType({self.name!r}, optional={self.optional})")
        return f"SchemaType({self.name!r}, optional={self.optional})"

    def __str__(self):
        return f"optional[{self.name}]" if self.optional else self.name

    def __eq__(self, other):
        if isinstance(other, SchemaType):
            return self.name == other.name and self.optional == other.optional
        if isinstance(other, str):
            log_i("eq", str(self))
            return str(self) == other.lower()
        return False


if __name__ == "__main__":  # pragma: no cover
    print(SchemaType("TIMESTAMPTZ", optional=False).name)
    print(str(SchemaType("uuid", optional=True)))
