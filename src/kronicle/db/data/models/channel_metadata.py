# kronicle/db/data/models/channel_metadata.py
from __future__ import annotations

from json import dumps, loads
from typing import Any, ClassVar
from uuid import UUID, uuid4

from asyncpg import Record, UniqueViolationError
from asyncpg.pool import PoolConnectionProxy
from pydantic import BaseModel, Field, ValidationInfo, field_validator

from kronicle.db.data.models.channel_schema import ChannelSchema
from kronicle.errors.error_types import BadRequestError, ConflictError, DatabaseInstructionError, NotFoundError
from kronicle.schemas.payload.processed_payload import ProcessedPayload
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.types.tag_type import TagType
from kronicle.utils.asyncpg_utils import table_exists
from kronicle.utils.dev_logs import log_d, log_e
from kronicle.utils.str_utils import ensure_uuid4, normalize_name, normalize_to_snake_case

mod = "chan_meta"


# --------------------------------------------------------------------------------------------------
# ChannelMetadata
# --------------------------------------------------------------------------------------------------
class ChannelMetadata(BaseModel):
    """
    Part of the payload that describes the data.
    One metadata row identifies one peculiar channel stream.
    `channel_id` identifies uniquely the channel stream from which we collect data.
    `received_at` is an auto-generated datetime tag that stores the date the user requested the metadata to be
        created.
    `channel_schema` describes the columns schema, i.e. the types of the columns of data in
        Python-like normalized application types (aka SchemaType). It is immutable (once created, it will not be updated)
    Optional `metadata` and `tag` are user-defined fields that can be used to add further
        information on the data.

    """

    channel_id: UUID
    channel_schema: ChannelSchema
    name: str | None = None
    user_metadata: dict[str, Any] | None = Field(default_factory=dict)
    tags: dict[str, TagType] | None = Field(default_factory=dict)
    received_at: IsoDateTime = Field(default_factory=lambda: IsoDateTime.now_local())

    _NAMESPACE: ClassVar[str] = "data"
    _TABLE_NAME: ClassVar[str] = "channel_metadata"
    _TABLE_SCHEMA: ClassVar[dict[str, str]] = {
        "channel_id": "UUID PRIMARY KEY",
        "channel_schema": "JSONB NOT NULL",
        "name": "TEXT UNIQUE",
        "user_metadata": "JSONB",
        "tags": "JSONB",
        "received_at": "TIMESTAMPTZ NOT NULL DEFAULT now()",
    }

    # ----------------------------------------------------------------------------------------------
    # Field validators
    # ----------------------------------------------------------------------------------------------
    @field_validator("channel_id", mode="before")
    @classmethod
    def ensure_uuid4(cls, v) -> UUID:
        return ensure_uuid4(v)

    @field_validator("name", mode="before")
    @classmethod
    def normalize_name(cls, s) -> str | None:
        if not s:
            return None
        return normalize_to_snake_case(s)

    @field_validator("user_metadata", "tags", mode="before")
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

    # ----------------------------------------------------------------------------------------------
    # Field accessors
    # ----------------------------------------------------------------------------------------------
    @classmethod
    def namespace(cls):
        """Read-only access to the namespace."""
        return str(cls._NAMESPACE)

    # @classmethod
    # def table_args(cls):
    #     """Return SQL schema info dynamically."""
    #     return {"schema": cls.namespace()}

    @classmethod
    def table_name(cls):
        return str(cls._TABLE_NAME)

    @classmethod
    def table_schema(cls) -> dict[str, str]:
        """Read-only access to the table schema dict."""
        return dict(cls._TABLE_SCHEMA)  # return a copy to avoid mutation

    @classmethod
    def get_schema_defs(cls) -> str:
        """Return SQL column definitions for CREATE TABLE."""
        return ", ".join(f"{col} {typ}" for col, typ in cls.table_schema().items())

    @classmethod
    def get_schema_columns(cls) -> list[tuple[str, str]]:
        """Return ordered list of (column, type) tuples."""
        return list(cls.table_schema().items())

    # ----------------------------------------------------------------------------------------------
    # DB row helpers
    # ----------------------------------------------------------------------------------------------
    def db_ready_values(self) -> list:
        """
        Return the values in a format ready to be inserted into PostgreSQL.
        JSONB fields are passed as dicts, not strings.
        """
        return [
            self.channel_id,
            self.channel_schema.to_db_json(),  # asyncpg will handle dict -> JSONB
            self.name or "",
            self.user_metadata or {},  # dict for JSONB
            self.tags or {},  # dict for JSONB
            self.received_at,
        ]

    @classmethod
    def from_db(cls, row: dict) -> ChannelMetadata:
        """Create ChannelMetadata from a DB row dict."""
        channel_id = row["channel_id"]
        name = row.get("name") or ""
        metadata = row.get("user_metadata") or {}
        tags = row.get("tags") or {}
        received = row.get("received_at")
        received_iso = IsoDateTime.to_iso_datetime(received) if received is not None else IsoDateTime.now_local()
        channel_schema = ChannelSchema.from_db_json(
            loads(row["channel_schema"]) if isinstance(row["channel_schema"], str) else row["channel_schema"]
        )
        return cls(
            channel_id=channel_id,
            channel_schema=channel_schema,
            name=name,
            user_metadata=metadata if isinstance(metadata, dict) else loads(metadata),
            tags=tags if isinstance(tags, dict) else loads(tags),
            received_at=received_iso,
        )

    @classmethod
    def from_processed(
        cls,
        processed: ProcessedPayload,
        channel_truth: ChannelSchema | None = None,
    ) -> ChannelMetadata:
        """
        Create a ChannelMetadata object from ProcessedPayload.
        Handles normalization and validation of metadata, tags, name, and schema.
        """
        channel_id = ensure_uuid4(processed.channel_id)
        if channel_truth:
            if processed.channel_schema:
                if channel_truth.equivalent_to(processed.channel_schema):
                    raise BadRequestError(
                        "This payload is not compatible with the existing channel schema",
                        details={
                            "channel_id": channel_id,
                            "channel_schema": {
                                "expected": channel_truth,
                                "received": processed.channel_schema,
                            },
                        },
                    )
            channel_schema = channel_truth
        else:
            channel_schema = processed.channel_schema
        name = normalize_name(processed.name, "channel_") if processed.name else ""

        return cls(
            channel_id=channel_id,
            name=name,
            channel_schema=channel_schema,
            user_metadata=processed.metadata or {},
            tags=processed.tags or {},
        )

    def update_with_db(self, row: Record):
        db_obj = type(self).from_db(dict(row))
        for field in type(self).model_fields:  # <-- class-level access!
            setattr(self, field, getattr(db_obj, field))
        log_d(mod, f"ChannelMetadata updated in place: {self.channel_id}")
        return self

    # ----------------------------------------------------------------------------------------------
    # DB operations: table
    # ----------------------------------------------------------------------------------------------
    @classmethod
    async def table_exists(cls, conn: PoolConnectionProxy) -> bool:
        """Return True if the ChannelMetadata table exists."""
        return await table_exists(conn, namespace=cls.namespace(), table_name=cls.table_name())

    @classmethod
    def create_table_sql(cls) -> str:
        """Return SQL to create table in Postgres."""
        return f"CREATE TABLE {cls.namespace()}.{cls.table_name()} ({cls.get_schema_defs()});"

    @classmethod
    async def ensure_table(cls, conn: PoolConnectionProxy):
        """Ensure the table exists in the database."""
        if await cls.table_exists(conn):
            return
        await conn.execute(cls.create_table_sql())
        log_d("ChannelMetadata.ensure_table", "Table created:", cls.table_name())

    # ----------------------------------------------------------------------------------------------
    # DB operations: read/fetch
    # ----------------------------------------------------------------------------------------------
    async def exists(self, conn: PoolConnectionProxy):
        return (await self.fetch_by_id(conn, self.channel_id)) is not None

    @classmethod
    async def fetch_by_id(cls, conn: PoolConnectionProxy, channel_id: UUID) -> ChannelMetadata | None:
        """
        Fetch a metadata row by channel_id.

        Args:
            conn: asyncpg.Connection object
            channel_id: UUID of the channel
        Returns:
            ChannelMetadata instance or None if not found
        """
        ensure_uuid4(channel_id)
        sql = f"""
        SELECT * FROM {cls.namespace()}.{cls.table_name()} WHERE channel_id = $1
        """
        row = await conn.fetchrow(sql, channel_id)
        if not row:
            return None
        return cls.from_db(dict(row))

    @classmethod
    async def fetch_by_name(cls, conn: PoolConnectionProxy, name: str) -> ChannelMetadata | None:
        """
        Fetch a metadata row by name.

        Args:
            conn: asyncpg.Connection object
            name: str, name of the channel (will be normalized)

        Returns:
            ChannelMetadata instance or None if not found
        """
        normalized_name = normalize_to_snake_case(name)
        sql = f"""
        SELECT * FROM {cls.namespace()}.{cls.table_name()}
        WHERE name = $1
        """
        row = await conn.fetchrow(sql, normalized_name)
        if not row:
            return None
        return cls.from_db(dict(row))

    @classmethod
    async def fetch_by_tags(cls, conn: PoolConnectionProxy, tags: dict[str, TagType]) -> list[ChannelMetadata]:
        """
        Fetch metadata rows that match all specified tags.

        Args:
            conn: asyncpg.Connection object
            tags: dict of tag_key -> tag_value to match (all must match)

        Returns:
            List of ChannelMetadata instances matching all tags
        """
        if not tags:
            return []

        # Normalize all keys
        normalized_tags = {
            normalize_to_snake_case(k): normalize_to_snake_case(v) if isinstance(v, str) else v for k, v in tags.items()
        }
        tag_filter = dumps(normalized_tags)

        sql = f"""
        SELECT * FROM {cls.namespace()}.{cls.table_name()}
        WHERE tags @> $1
        ORDER BY received_at DESC
        """
        rows = await conn.fetch(sql, tag_filter)
        return [cls.from_db(dict(r)) for r in rows]

    @classmethod
    async def fetch_all(cls, conn: PoolConnectionProxy) -> list[ChannelMetadata]:
        """
        Fetch all metadata rows, ordered by received_at descending.

        Args:
            conn: asyncpg.Connection object
            filter: optional RequestFilter to limit rows or paginate results

        Returns:
            List of ChannelMetadata objects
        """
        sql = f"""
        SELECT * FROM {cls.namespace()}.{cls.table_name()} ORDER BY received_at DESC
        """
        rows = await conn.fetch(sql)
        return [cls.from_db(dict(r)) for r in rows]

    # ----------------------------------------------------------------------------------------------
    # DB operations: create
    # ----------------------------------------------------------------------------------------------
    async def create(self, conn: PoolConnectionProxy) -> ChannelMetadata:
        """
        Insert a new ChannelMetadata row.
        Raises an error if the channel_id already exists.
        """
        # ChannelMetadata table is supposed to exist (checked at bootstrap)
        here = f"{mod}.create"
        # Check if metadata exists
        exists = await self.fetch_by_id(conn, self.channel_id)
        if exists:
            raise ConflictError("ChannelMetadata already exists", details={"channel_id": str(self.channel_id)})
        columns = list(self.table_schema().keys())
        placeholders = [f"${i+1}" for i in range(len(columns))]
        sql = f"""
        INSERT INTO {self.namespace()}.{self.table_name()} ({', '.join(columns)})
        VALUES ({', '.join(placeholders)});
        RETURNING *;
        """
        try:
            row = await conn.fetchrow(sql, *self.db_ready_values())
            if row is None:
                log_e(here, f"INSERT did not return a row - channel_id={self.channel_id}")
                raise DatabaseInstructionError("INSERT did not return a row", details={"channel_id": self.channel_id})
            log_d(mod, f"ChannelMetadata created: {self.channel_id}")
            return self.update_with_db(row)
        except UniqueViolationError as e:
            # Determine which field caused the violation
            constraint = getattr(e, "constraint_name", "")  # asyncpg sets this if the DB reports the constraint name
            if constraint == f"{self.table_name()}_pkey":
                raise ConflictError(
                    "ChannelMetadata with this ID already exists", details={"channel_id": str(self.channel_id)}
                ) from e
            elif constraint == f"{self.table_name()}_name_key":
                raise ConflictError("ChannelMetadata with this name already exists", details={"name": self.name}) from e
            else:
                # fallback: include original error args
                raise ConflictError(
                    "ChannelMetadata violates a unique constraint",
                    details={"channel_id": str(self.channel_id), "name": self.name},
                ) from e

    # ----------------------------------------------------------------------------------------------
    # DB operations: update
    # ----------------------------------------------------------------------------------------------
    async def update(self, conn: PoolConnectionProxy) -> ChannelMetadata:
        """
        Update mutable fields of an existing ChannelMetadata.
        Immutable fields: channel_id, channel_schema
        Mutable fields: name, user_metadata, tags
        """
        # ChannelMetadata table is supposed to exist (checked at bootstrap)

        # Fetch existing metadata
        existing = await self.fetch_by_id(conn, self.channel_id)
        if not existing:
            raise ValueError(f"No ChannelMetadata found for id {self.channel_id}")

        columns = ["name", "user_metadata", "tags"]
        placeholders = [f"${i+1}" for i in range(len(columns) + 1)]  # +1 for WHERE

        sql = f"""
            UPDATE {self.namespace()}.{self.table_name()}
            SET {', '.join(f"{col} = {placeholders[idx]}" for idx, col in enumerate(columns))}
            WHERE channel_id = {placeholders[-1]}
            RETURNING *;
            """
        values = [getattr(self, col) if col == "name" else getattr(self, col) or {} for col in columns]
        values.append(self.channel_id)

        try:
            row = await conn.fetchrow(sql, *values)
            if row is None:
                raise NotFoundError("No ChannelMetadata found", details={"channel_id": self.channel_id})
            log_d(mod, f"ChannelMetadata updated: {self.channel_id}")
            # Return a new instance built from the updated row
            return self.update_with_db(row)
        except UniqueViolationError as e:
            constraint = getattr(e, "constraint_name", "")
            if constraint == f"{self.table_name()}_name_key":
                raise ConflictError("Another channel with this name already exists", details={"name": self.name}) from e
            else:
                raise ConflictError(
                    "ChannelMetadata violates a unique constraint",
                    details={"channel_id": str(self.channel_id), "name": self.name},
                ) from e

    # ----------------------------------------------------------------------------------------------
    # DB operations: delete
    # ----------------------------------------------------------------------------------------------
    async def delete(self, conn: PoolConnectionProxy) -> ChannelMetadata | None:
        """
        Delete this channel from the database.

        Returns:
            The deleted ChannelMetadata instance, or None if it didn't exist.
        """
        existing = await self.fetch_by_id(conn, self.channel_id)
        if not existing:
            return None

        sql = f"""
        DELETE FROM {self.namespace()}.{self.table_name()}
        WHERE channel_id = $1
        RETURNING *;
        """
        row = await conn.fetchrow(sql, self.channel_id)
        if not row:
            return None  # row didn't exist
        return self.update_with_db(row)


# --------------------------------------------------------------------------------------------------
# Main test
# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":  # pragma: no cover

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
        name="meta1",
        user_metadata={"location": "lab"},
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

    for col, col_type in ChannelMetadata.get_schema_columns():
        log_d(here, f"type of schema column `{col}`", col_type)

if __name__ == "__main__":  # pragma: no cover
    print(ChannelMetadata.namespace())  # prints data => OK
