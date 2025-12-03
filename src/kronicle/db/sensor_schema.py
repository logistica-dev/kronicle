# kronicle/db/sensor_schema.py
from __future__ import annotations

from datetime import datetime
from json import dumps
from typing import Tuple

from pydantic import BaseModel, Field

from kronicle.types.iso_datetime import IsoDateTime
from kronicle.types.schema_types import SchemaType
from kronicle.utils.dev_logs import log_d, log_w
from kronicle.utils.str_utils import normalize_column_name

mod = "sensor_schema"

# fmt: off
RESERVED_SQL_KEYWORDS = {
    # PostgreSQL standard keywords
    "user", "select", "insert", "update", "delete", "join", "group", "order", "limit", "values", "table", "index",
    # TimescaleDB-specific / hypertable keywords
    "chunk", "compress", "policy", "partition",
    }
# fmt: on


# --------------------------------------------------------------------------------------------------
# SensorSchema
# --------------------------------------------------------------------------------------------------
class SensorSchema(BaseModel):
    """
    Stores application-level schema.
    columns: dict[column_name, SchemaType]
    db_to_usr: dict[db_column_name, user_column_name]
    """

    column_types: dict[str, SchemaType] = Field(..., description="Column name -> SchemaType")
    db_to_usr: dict[str, str] = Field(
        default_factory=dict, description="DB column name mapping -> User-defined column name"
    )

    # @field_validator("columns")
    # @classmethod
    # def validate_column_names(cls, v: dict[str, SchemaType]) -> dict[str, SchemaType]:
    #     for name in v:
    #         if not name.isidentifier():
    #             raise ValueError(f"Invalid column name: {name}")
    #     return v
    @property
    def user_columns(self) -> dict[str, SchemaType]:
        return {c: ct for c, ct in self.column_types.items() if c not in ("time", "received_at")}

    @property
    def ordered_columns(self) -> list[str]:
        return ["time"] + list(self.user_columns.keys()) + ["received_at"]

    @property
    def sql_columns(self) -> list[str]:
        return ["row_id", "time"] + list(self.user_columns.keys()) + ["received_at"]

    @classmethod
    def sanitize_user_schema(cls, schema_dict: dict[str, str]) -> "SensorSchema":
        """
        Sanitize a user-provided schema before storing:
        - Validates column names (non-empty, no whitespace-only, normalized)
        - Maps user types to SchemaType
        - Raises ValueError if any invalid type or column name is found
        """
        sanitized_col_types: dict[str, SchemaType] = {}
        db_to_usr: dict[str, str] = {}

        for usr_col, usr_type in schema_dict.items():
            # --- Validate column name ---
            if not isinstance(usr_col, str) or not usr_col.strip():
                raise ValueError(f"Invalid column name: '{usr_col}'")

            # Normalization
            db_col = normalize_column_name(usr_col)

            # Skip received_at silently
            if db_col == "received_at":
                continue

            # Enforce datetime type for "time"
            if db_col == "time":
                sanitized_col_types[db_col] = SchemaType.from_user_type("datetime")
                db_to_usr[db_col] = usr_col
                continue

            # Check for duplicates
            if db_col in sanitized_col_types:
                raise ValueError(f"Duplicate normalized column name detected: '{db_col}' (from '{usr_col}')")

            # Avoid SQL/Timescale reserved keywords
            if db_col in RESERVED_SQL_KEYWORDS:
                raise ValueError(f"Column name '{db_col}' is reserved")  # by SQL/TimescaleDB

            # --- Map type ---
            sanitized_col_types[db_col] = SchemaType.from_user_type(usr_type)
            db_to_usr[db_col] = usr_col

        if not sanitized_col_types:
            raise ValueError("Schema cannot be empty")

        return cls(column_types=sanitized_col_types, db_to_usr=db_to_usr)

    @classmethod
    def from_user_json(cls, schema_dict: dict[str, str]) -> "SensorSchema":
        """
        Convert user schema (user names + type strings) to DB-safe names and SchemaType.
        """
        return cls.sanitize_user_schema(schema_dict)

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

    def equivalent_to(self, other: SensorSchema) -> bool:
        """
        Compare this schema to another, ignoring user-defined names.
        Returns True if the set of normalized column names and their types match.
        """
        if not isinstance(other, SensorSchema):
            return False

        # Compare column names and types only
        return self.column_types == other.column_types

    def diff(self, other: SensorSchema) -> dict[str, tuple[str, str]]:
        """
        Return a diff mapping for mismatched columns:
        {column_name: (this_type, other_type)}
        """
        diff = {}
        all_keys = set(self.column_types) | set(other.column_types)
        for k in all_keys:
            t1 = str(self.column_types.get(k))
            t2 = str(other.column_types.get(k))
            if t1 != t2:
                diff[k] = (t1, t2)
        return diff

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # SQL helpers
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    @property
    def sql_table_definition(self) -> str:
        """
        Return SQL-ready table definition for CREATE TABLE.
        """
        usr_cols = [f"{c} {c_type.db_type}" for c, c_type in self.user_columns.items()]
        table_def = [
            "row_id BIGSERIAL",
            "time TIMESTAMPTZ NOT NULL",
            *usr_cols,
            "received_at TIMESTAMPTZ NOT NULL DEFAULT now()",
            "PRIMARY KEY (row_id, time)",
        ]
        return ", ".join(table_def)

    def compare_with_db_columns(self, db_cols: dict[str, str]) -> None:
        """
        Compare this schema with columns present in DB.
        Raises ValueError if mismatch is found.
        - db_cols: {col_name: db_type, ...} from information_schema
        """
        here = f"{mod}.compare_with_db_columns"
        for col, col_type in self.column_types.items():
            db_type = db_cols.get(col)
            expected = col_type.db_type.upper()
            # Use db_to_usr mapping to show user-friendly column name
            user_col_name = self.get_usr_col_name(col)
            if db_type is None:
                raise ValueError(f"{here}: Missing column '{user_col_name}' in DB")
            if db_type.upper() != expected:
                if expected.startswith("TIMESTAMP") and db_type.upper().startswith("TIMESTAMP"):
                    continue
                raise ValueError(
                    f"{here}: Column '{user_col_name}' type mismatch (expected {expected}, found {db_type})"
                )

        # Optionally warn about extra columns in DB
        for db_col in db_cols.keys():
            if db_col not in self.sql_columns:
                log_w(here, f"Extra column in DB not in schema: {db_col}")

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Row validation
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def validate_row(self, row: dict, now: datetime | None = None, from_user: bool = True) -> dict:
        here = f"{mod}.validate_row"
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
                log_w(here, "Missing column", db_col)
                log_w(here, "Row content", row)
                if col_type.optional:
                    continue
                raise ValueError(f"Missing column '{user_col or db_col}' in row")

            val = row[key_in_row]

            # Validate JSON types (dict/list) or other types
            validated[db_col] = col_type.validate(val, db_col)

        if not from_user and (timestamp := row.get("received_at")):
            validated["received_at"] = timestamp
        else:
            validated["received_at"] = now

        return validated

    def rows_to_db_tuples(
        self, rows: list[dict], *, include_time=True, include_received_at=True, log_errors=True
    ) -> Tuple[list[Tuple], list[str]]:
        """
        Convert rows to DB-ready tuples, validating each row.
        Returns (valid_tuples, errors).
        Invalid rows are skipped but errors are collected.
        """
        here = f"{mod}.rows_to_db_tuples"
        cols = list(self.user_columns.keys())
        if include_time:
            cols = ["time"] + cols
        if include_received_at:
            cols += ["received_at"]

        valid_tuples: list[Tuple] = []
        errors: list[str] = []

        for i, row in enumerate(rows):
            try:
                validated = self.validate_row(row, from_user=True)
                valid_tuples.append(tuple(validated[c] for c in cols))
                # if log_errors:
                #     log_d(here, f"Validated row {i}", validated)
            except Exception as e:
                err_msg = f"Row {i} invalid: {e}"
                errors.append(err_msg)
                if log_errors:
                    log_w(here, err_msg)

        return valid_tuples, errors

    def db_row_to_user_row(self, row: dict, *, skip_received: bool = True) -> dict:
        """Convert a single DB-keyed row to user-defined column names."""
        skip_fields = {"received_at"}
        user_row = {}

        for db_col, val in row.items():
            if skip_received and db_col in skip_fields:
                continue
            user_col = self.get_usr_col_name(db_col)
            if isinstance(val, IsoDateTime) or self.column_types.get(db_col) == "datetime":
                user_row[user_col] = IsoDateTime.normalize_value(val, to_local_tz=True)
            else:
                user_row[user_col] = val
        return user_row

    def db_cols_to_user_cols(self, db_cols: dict[str, list]) -> dict:
        """Convert a single DB-keyed row to user-defined column names."""
        skip_fields = {"received_at"}
        user_cols = {}

        for col_name, values in db_cols.items():
            if col_name in skip_fields:
                continue
            if len(values) == 0:
                continue
            user_col_name = self.get_usr_col_name(col_name)
            if isinstance(values[0], IsoDateTime) or self.column_types.get(col_name) == "datetime":
                user_cols[user_col_name] = [IsoDateTime.normalize_value(val, to_local_tz=True) for val in values]
            else:
                user_cols[user_col_name] = values
        return user_cols

    def db_rows_to_user_rows(self, rows: list[dict], *, skip_received: bool = True) -> list[dict]:
        """Convert list of DB-keyed rows to user-defined column names."""
        return [self.db_row_to_user_row(row, skip_received=skip_received) for row in rows]

    def db_rows_to_user_columns(self, rows: list[dict]) -> dict[str, list]:
        """
        Convert a list of DB-keyed rows into a column-oriented dict using user-defined names.
        Example: [{"a":1,"b":2},{"a":3,"b":4}] -> {"a":[1,3], "b":[2,4]}
        Ensure datetime fields are converted to local timezone via IsoDateTime.

        """
        here = f"{mod}.db_rows_to_usr_cols"
        if not rows:
            return {}
        log_d(here, "rows", rows)
        # Initialize column lists
        columns: dict[str, list] = {self.get_usr_col_name(k): [] for k in rows[0].keys()}

        for row in rows:
            for db_col, val in row.items():
                user_col = self.get_usr_col_name(db_col)
                if self.column_types.get(db_col) == "datetime":
                    val = IsoDateTime.normalize_value(val, to_local_tz=True)
                columns[user_col].append(val)

        return columns


# --------------------------------------------------------------------------------------------------
# Main test
# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":

    from kronicle.utils.dev_logs import log_d

    here = "sensor_schema tests"

    # Define a user schema with mixed-case type names
    user_schema = {
        "temperature": "Number",
        "humidity": "float",
        "meta": "dict",
        "tags": "LIST",
    }

    schema = SensorSchema.from_user_json(user_schema)

    log_d(here, "User JSON", schema.to_user_json())
    log_d(here, "DB JSON", schema.to_db_json())
    log_d(here, "Ordered columns", schema.ordered_columns)
    log_d(here, "SQL table definition", schema.sql_table_definition)

    # Example row
    sample_row = {
        "time": IsoDateTime.now_local(),
        "temperature": 25.5,
        "humidity": 50.2,
        "meta": {"note": "ok"},
        "tags": [1, 2, 3],
    }

    validated_row = schema.validate_row(sample_row)
    log_d(here, "Validated row", validated_row)

    tuples, errors = schema.rows_to_db_tuples([sample_row])
    log_d(here, "Tuples for DB", tuples)
    log_d(here, "Errors", errors)

    # Add an invalid row (missing humidity)
    bad_row = {
        "time": IsoDateTime.now_local(),
        "temperature": 26.0,
        "meta": {"note": "missing humidity"},
        "tags": [9],
    }

    tuples, errors = schema.rows_to_db_tuples([bad_row])
    log_d(here, "Tuples for DB (bad row)", tuples)
    log_d(here, "Errors (bad row)", errors)

    # Simulate DB columns
    fake_db_cols = {
        "temperature": "DOUBLE PRECISION",
        "humidity": "DOUBLE PRECISION",
        "meta": "JSONB",
        # Intentionally wrong type to see mismatch
        "tags": "TEXT",
    }

    try:
        schema.compare_with_db_columns(fake_db_cols)
    except ValueError as e:
        log_w(here, f"Caught (expected) error - Schema mismatch detected: {e}")

    # Add an extra column in DB to see the warning
    fake_db_cols_extra = fake_db_cols.copy()
    fake_db_cols_extra["tags"] = "JSONB"
    fake_db_cols_extra["extra_column"] = "TEXT"
    schema.compare_with_db_columns(fake_db_cols_extra)
