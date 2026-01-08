# kronicle.db.channel_metadata.py
from __future__ import annotations

from json import loads
from typing import Any, ClassVar, Tuple
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from kronicle.db.channel_schema import ChannelSchema
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.utils.str_utils import check_is_uuid4, ensure_uuid4, normalize_to_snake_case

mod = "channel_metadata"


# --------------------------------------------------------------------------------------------------
# ChannelMetadata
# --------------------------------------------------------------------------------------------------
class ChannelMetadata(BaseModel):
    """
    Part of the payload that describes the data.
    One metadata row identifies one peculiar channel stream.
    `channel_id` identifies uniquely the channel stream from which we collect data.
    `received_at`is a datetime tag that stores the date the user requested the metadata to be
        created.
    `channel_schema` describes the columns schema, i.e. the types of the columns of data in
        Python-like normalized application types (aka SchemaType).
    Optional `metadata` and `tag` are user-defined fields that can be used to add further
        information on the data.

    """

    channel_id: UUID
    channel_schema: ChannelSchema
    channel_name: str | None = None
    metadata: dict[str, Any] | None = Field(default_factory=dict)
    tags: dict[str, str | int | float | list] | None = Field(default_factory=dict)
    received_at: IsoDateTime = Field(default_factory=lambda: IsoDateTime.now_local())

    _TABLE_SCHEMA: ClassVar[dict[str, str]] = {
        "channel_id": "UUID PRIMARY KEY",
        "channel_schema": "JSONB NOT NULL",
        "channel_name": "TEXT",
        "metadata": "JSONB",
        "tags": "JSONB",
        "received_at": "TIMESTAMPTZ NOT NULL DEFAULT now()",
    }

    @field_validator("channel_id", mode="before")
    @classmethod
    def ensure_uuid4(cls, v) -> UUID:
        return ensure_uuid4(v)

    @field_validator("channel_name", mode="before")
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
    def get_table_name_for_channel(channel_id: UUID) -> str:
        return f"channel_{check_is_uuid4(channel_id).replace('-', '')}"

    @property
    def data_table_name(self) -> str:
        return self.get_table_name_for_channel(self.channel_id)

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
            self.channel_id,
            self.channel_schema.to_user_json(),  # asyncpg will handle dict -> JSONB
            self.channel_name or "",
            self.metadata or {},  # dict for JSONB
            self.tags or {},  # dict for JSONB
            self.received_at,
        ]

    @classmethod
    def from_db(cls, row: dict) -> ChannelMetadata:
        channel_id = row["channel_id"]
        channel_name = row.get("channel_name") or ""
        metadata = row.get("metadata") or {}
        tags = row.get("tags") or {}
        received = row.get("received_at")
        received_iso = IsoDateTime.to_iso_datetime(received) if received is not None else IsoDateTime.now_local()
        channel_schema = ChannelSchema.from_user_json(
            loads(row["channel_schema"]) if isinstance(row["channel_schema"], str) else row["channel_schema"]
        )
        return cls(
            channel_id=channel_id,
            channel_schema=channel_schema,
            channel_name=channel_name,
            metadata=metadata if isinstance(metadata, dict) else loads(metadata),
            tags=tags if isinstance(tags, dict) else loads(tags),
            received_at=received_iso,
        )


# --------------------------------------------------------------------------------------------------
# Main test
# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    from kronicle.utils.dev_logs import log_d

    here = "channel_schema tests"

    user_schema = {
        "time": "time",
        "temperature": "Number",
        "humidity": "FLOAT",
        "meta": "dict",
        "tags": "list",
    }
    channel_schema = ChannelSchema.from_user_json(user_schema)

    log_d(here, "User JSON", channel_schema.to_user_json())
    log_d(here, "DB JSON", channel_schema.to_db_json())
    log_d(here, "Ordered columns", channel_schema.ordered_columns)

    metadata = ChannelMetadata(
        channel_id=uuid4(),
        channel_schema=channel_schema,
        channel_name="meta1",
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
    validated_row = channel_schema.validate_row(sample_row)
    log_d(here, "Validated row", validated_row)

    tuples, errors = channel_schema.rows_to_db_tuples([sample_row])
    log_d(here, "Tuples for DB", tuples)

    for col, col_type in ChannelMetadata.get_schema_columns():
        log_d(here, f"type of schema column `{col}`", col_type)
