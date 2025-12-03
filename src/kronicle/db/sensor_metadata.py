# kronicle/db/sensor_metadata.py
from __future__ import annotations

from json import loads
from typing import Any, ClassVar, Tuple
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from kronicle.db.sensor_schema import SensorSchema
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.utils.str_utils import check_is_uuid4, ensure_uuid4, normalize_to_snake_case

mod = "sensor_metadata"


# --------------------------------------------------------------------------------------------------
# SensorMetadata
# --------------------------------------------------------------------------------------------------
class SensorMetadata(BaseModel):
    """
    Part of the payload that describes the data.
    One metadata row identifies one peculiar sensor stream.
    `sensor_id` identifies uniquely the sensor stream from which we collect data.
    `received_at`is a datetime tag that stores the date the user requested the metadata to be
        created.
    `sensor_schema` describes the columns schema, i.e. the types of the columns of data in
        Python-like normalized application types (aka SchemaType).
    Optional `metadata` and `tag` are user-defined fields that can be used to add further
        information on the data.

    """

    sensor_id: UUID
    sensor_schema: SensorSchema
    sensor_name: str | None = None
    metadata: dict[str, Any] | None = Field(default_factory=dict)
    tags: dict[str, str | int | float | list] | None = Field(default_factory=dict)
    received_at: IsoDateTime = Field(default_factory=lambda: IsoDateTime.now_local())

    _TABLE_SCHEMA: ClassVar[dict[str, str]] = {
        "sensor_id": "UUID PRIMARY KEY",
        "sensor_schema": "JSONB NOT NULL",
        "sensor_name": "TEXT",
        "metadata": "JSONB",
        "tags": "JSONB",
        "received_at": "TIMESTAMPTZ NOT NULL DEFAULT now()",
    }

    @field_validator("sensor_id", mode="before")
    @classmethod
    def ensure_uuid4(cls, v) -> UUID:
        return ensure_uuid4(v)

    @field_validator("sensor_name", mode="before")
    @classmethod
    def normalize_name(cls, s) -> str:
        return normalize_to_snake_case(s)

    @field_validator("metadata", "tags", mode="before")
    @classmethod
    def ensure_dict_or_none(cls, v: dict | None, info: ValidationInfo) -> dict:
        if v is None:
            return {}
        if not isinstance(v, dict):
            raise TypeError(f"{info.field_name} must be a dict[str, int|float|str] or None")
        for key in v.keys():
            if not key.strip():
                raise ValueError("Tag key cannot be empty")
        return v

    @staticmethod
    def get_table_name_for_sensor(sensor_id: UUID) -> str:
        return f"sensor_{check_is_uuid4(sensor_id).replace('-', '')}"

    @property
    def data_table_name(self) -> str:
        return self.get_table_name_for_sensor(self.sensor_id)

    @classmethod
    def get_table_schema(cls) -> dict[str, str]:
        """Read-only access to the table schema dict."""
        return dict(cls._TABLE_SCHEMA)  # return a copy to avoid mutation

    @classmethod
    def get_schema_defs(cls) -> str:
        """Return SQL column definitions for CREATE TABLE."""
        return ", ".join(f"{col} {typ}" for col, typ in cls._TABLE_SCHEMA.items())

    @classmethod
    def get_schema_columns(cls) -> list[Tuple[str, str]]:
        """Return ordered list of (column, type) tuples."""
        return list(cls._TABLE_SCHEMA.items())

    # ----------------------------------------------------------------------------------------------
    # JSON helpers
    # ----------------------------------------------------------------------------------------------
    def db_ready_values(self) -> list:
        """
        Return the values in a format ready to be inserted into PostgreSQL.
        JSONB fields are passed as dicts, not strings.
        """

        return [
            self.sensor_id,
            self.sensor_schema.to_user_json(),  # asyncpg will handle dict -> JSONB
            self.sensor_name or "",
            self.metadata or {},  # dict for JSONB
            self.tags or {},  # dict for JSONB
            self.received_at,
        ]

    @classmethod
    def from_db(cls, row: dict) -> SensorMetadata:
        sensor_id = row["sensor_id"]
        sensor_name = row.get("sensor_name") or ""
        metadata = row.get("metadata") or {}
        tags = row.get("tags") or {}
        received = row.get("received_at")
        received_iso = IsoDateTime.to_iso_datetime(received) if received is not None else IsoDateTime.now_local()
        sensor_schema = SensorSchema.from_user_json(
            loads(row["sensor_schema"]) if isinstance(row["sensor_schema"], str) else row["sensor_schema"]
        )
        return cls(
            sensor_id=sensor_id,
            sensor_schema=sensor_schema,
            sensor_name=sensor_name,
            metadata=metadata if isinstance(metadata, dict) else loads(metadata),
            tags=tags if isinstance(tags, dict) else loads(tags),
            received_at=received_iso,
        )


# --------------------------------------------------------------------------------------------------
# Main test
# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    from kronicle.utils.dev_logs import log_d

    here = "sensor_schema tests"

    user_schema = {
        "time": "time",
        "temperature": "Number",
        "humidity": "FLOAT",
        "meta": "dict",
        "tags": "list",
    }
    sensor_schema = SensorSchema.from_user_json(user_schema)

    log_d(here, "User JSON", sensor_schema.to_user_json())
    log_d(here, "DB JSON", sensor_schema.to_db_json())
    log_d(here, "Ordered columns", sensor_schema.ordered_columns)

    metadata = SensorMetadata(
        sensor_id=uuid4(),
        sensor_schema=sensor_schema,
        sensor_name="meta1",
        metadata={"location": "lab"},
        tags={"room": 101},
    )

    log_d(here, "metadata", metadata)
    log_d(here, "DB ready values", metadata.db_ready_values())

    sample_row = {
        "time": IsoDateTime.now_local(),
        "temperature": 25.5,
        "humidity": 50.2,
        "meta": {"note": "ok"},
        "tags": [1, 2],
    }
    validated_row = sensor_schema.validate_row(sample_row)
    log_d(here, "Validated row", validated_row)

    tuples, errors = sensor_schema.rows_to_db_tuples([sample_row])
    log_d(here, "Tuples for DB", tuples)

    for col, col_type in SensorMetadata.get_schema_columns():
        log_d(here, f"type of schema column `{col}`", col_type)
