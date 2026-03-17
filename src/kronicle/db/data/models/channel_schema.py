# kronicle/db/data/models/channel_schema.py
from __future__ import annotations

from datetime import datetime
from json import dumps
from typing import Any

from pydantic import BaseModel, Field

from kronicle.db.data.models.schema_types import SchemaType
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.utils.str_utils import normalize_name, normalize_pg_identifier

mod = "chan_schm"


# fmt: off
RESERVED_SQL_KEYWORDS = {
    # PostgreSQL standard keywords
    "user", "select", "insert", "update", "delete", "join",
    "group", "order", "limit", "values", "table", "index",
    # TimescaleDB-specific / hypertable keywords
    "chunk", "compress", "policy", "partition",
    }
# fmt: on


class ChannelSchema(BaseModel):
    """
    ChannelSchema is a pure structural definition of a channel's timeseries layout.

    Responsibilities:
    - Define user-facing column names and their types.
    - Provide validation rules for rows.
    - Provide deterministic user↔internal column normalization.
    - Offer structural comparison (e.g. for migration/versioning).

    It is:
    - Storage-agnostic (no table name, no SQL, no DB types).
    - Immutable (once created, structure does not change).
    - Safe to serialize and embed in API payloads (part of ChannelMetadata).

    It does NOT:
    - Know about channel_id.
    - Generate SQL.
    - Know about row id or received_at.
    - Compare itself to a live database table.

    Database materialization is handled by ChannelTimeseries.
    """

    model_config = {"frozen": True, "extra": "forbid"}  # Prevent accidental schema mutation

    # Normalized DB column name -> SchemaType
    column_types: dict[str, SchemaType] = Field(..., description="Column name -> SchemaType")

    # Normalized DB column name -> original user column name
    db_to_usr: dict[str, str] = Field(
        default_factory=dict, description="DB column name mapping -> User-defined column name"
    )

    @property
    def user_columns(self) -> dict[str, SchemaType]:
        return {c: ct for c, ct in self.column_types.items() if c not in ("time", "received_at")}

    @property
    def ordered_columns(self) -> list[str]:
        return ["time"] + list(self.user_columns.keys()) + ["received_at"]

    @classmethod
    def sanitize_user_schema(cls, raw_schema: dict[str, str]) -> ChannelSchema:
        """
        Sanitize a user-provided schema before storing:
        - Validates column names (non-empty, no whitespace-only, normalized)
        - Maps user types to SchemaType
        - Deterministic ordering
        - Raises ValueError if any invalid type or column name is found
        """
        # log_i("ChannelSchema.sanitize_user_schema", raw_schema, "is not", not raw_schema)
        if not raw_schema:
            raise ValueError("Schema cannot be empty")

        sanitized_col_names: dict[str, SchemaType] = {}
        db_to_usr: dict[str, str] = {}

        for usr_col, usr_type in raw_schema.items():
            db_col, schema_type = cls._sanitize_column(usr_col, usr_type, sanitized_col_names)
            if db_col and schema_type:
                sanitized_col_names[db_col] = schema_type
                db_to_usr[db_col] = usr_col.strip()

        if not sanitized_col_names:
            raise ValueError("Schema cannot be empty")

        return cls(column_types=sanitized_col_names, db_to_usr=db_to_usr)

    @classmethod
    def _sanitize_column(
        cls, usr_col: str, usr_type: str, sanitized_col_names: dict[str, SchemaType]
    ) -> tuple[str | None, SchemaType | None]:
        """Validate and normalize a single column."""
        # --- Validate column name ---
        if not isinstance(usr_col, str):
            raise ValueError("Column names must be strings")

        name = usr_col.strip()
        if not name:
            raise ValueError("Column names cannot be empty")

        # Normalization
        db_col = normalize_name(name, "col_")
        if not db_col:
            raise ValueError("Incorrect column name")

        # Skip received_at silently
        if db_col == "received_at":
            return None, None

        # Enforce datetime type for "time"
        if db_col == "time":
            return db_col, SchemaType.from_str("datetime")

        # Check for duplicates
        if db_col in sanitized_col_names:
            raise ValueError(f"Duplicate normalized column name detected: '{db_col}' (from '{name}')")

        # Avoid SQL/Timescale reserved keywords
        if db_col in RESERVED_SQL_KEYWORDS:
            raise ValueError(f"Column name '{db_col}' is reserved")  # by SQL/TimescaleDB

        return normalize_pg_identifier(db_col), SchemaType.from_str(usr_type)

    @classmethod
    def from_user_json(cls, schema_dict: dict[str, str]) -> ChannelSchema:
        """
        Construct a ChannelSchema from raw user JSON payload.
        """
        return cls.sanitize_user_schema(schema_dict)

    @classmethod
    def from_db_json(cls, db_schema: dict[str, str]) -> ChannelSchema:
        """
        Reconstruct a ChannelSchema from DB-stored schema JSON.
        Uses canonical DB types rather than user-facing type strings.
        """
        column_types: dict[str, SchemaType] = {}
        db_to_usr: dict[str, str] = {}

        for col, db_type in db_schema.items():
            # Assume db_type is already a canonical DB type string
            column_types[col] = SchemaType.from_str(db_type)
            db_to_usr[col] = col  # No user names stored in DB, map to same

        return cls(column_types=column_types, db_to_usr=db_to_usr)

    def get_usr_col_name(self, db_col: str) -> str:
        return self.db_to_usr.get(db_col, db_col)

    def to_user_json(self) -> dict[str, str]:
        """
        Return user-facing schema using the user column names and Python type strings
        """
        return {self.get_usr_col_name(db_col): str(schema_type) for db_col, schema_type in self.column_types.items()}

    def to_db_json(self) -> dict[str, str]:
        """Return schema as DB-ready types"""
        return {col: app_type.db_type for col, app_type in self.column_types.items()}

    def model_dump(self, flatten: bool = True, **kwargs):
        # Return columns directly, skipping the "columns" wrapper
        if flatten:
            return self.to_user_json()
        return super().model_dump(**kwargs)

    def model_dump_json(self, flatten: bool = True, **kwargs):
        # Optional: also flatten for JSON dumps
        return dumps(self.model_dump(flatten=flatten, **kwargs))

    # ----------------------------------------------------------------------------------------------
    # Structural comparison
    # ----------------------------------------------------------------------------------------------

    def equivalent_to(self, other: ChannelSchema) -> bool:
        """
        Compare this schema to another, ignoring user-defined names.
        Returns True if the set of normalized column names and their types match.
        """
        if not isinstance(other, ChannelSchema):
            return False

        # Compare column names and types only
        return self.user_columns == other.user_columns

    def diff(self, other: ChannelSchema) -> dict[str, Any]:
        """
        Return structural differences between schemas.
        """
        return {
            "added": {k: v for k, v in other.user_columns.items() if k not in self.user_columns},
            "removed": {k: v for k, v in self.user_columns.items() if k not in other.user_columns},
            "changed": {
                k: (self.user_columns[k], other.user_columns[k])
                for k in self.user_columns
                if k in other.user_columns and self.user_columns[k] != other.user_columns[k]
            },
        }

    # ---------------------------------------------------------------------
    # Validation
    # ---------------------------------------------------------------------

    def validate_row(self, row: dict, now: datetime | None = None, from_user: bool = True) -> dict:
        now = now or IsoDateTime.now_local()

        # Handle `time` column
        time_val = IsoDateTime.normalize_value(t) if (t := row.get("time")) else now
        validated = {"time": time_val}

        for db_col, col_type in self.user_columns.items():
            user_col = self.get_usr_col_name(db_col)

            # Accept either the normalized DB name or the user-provided name
            if db_col in row:
                key_in_row = db_col
            elif user_col and user_col in row:
                key_in_row = user_col
            else:
                if col_type.optional:
                    # Auto-fill missing optional column with None
                    validated[db_col] = col_type.validate(None)
                    continue
                raise ValueError(f"Missing column '{user_col or db_col}' in row")

            val = row[key_in_row]

            # Validate JSON types (dict/list) or other types
            validated[db_col] = col_type.validate(val)

        if not from_user and (timestamp := row.get("received_at")):
            validated["received_at"] = timestamp
        else:
            validated["received_at"] = now

        return validated


if __name__ == "__main__":  # pragma: no cover
    chan_schema = ChannelSchema.from_user_json({"temp": "optional[float]", "room_id": "str"})
    print(chan_schema.to_db_json())
    print(chan_schema.to_user_json())
