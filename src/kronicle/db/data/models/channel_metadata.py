# kronicle/db/data/models/channel_metadata.py
from __future__ import annotations

from json import loads
from typing import Any, ClassVar
from uuid import UUID, uuid4

from asyncpg import Record, UniqueViolationError
from asyncpg.pool import PoolConnectionProxy
from pydantic import BaseModel, Field, PrivateAttr, ValidationInfo, field_validator, model_validator

from kronicle.db.data.models.channel_schema import ChannelSchema
from kronicle.db.data.models.column_metadata import ColumnMetadataMap, normalize_column_metadata
from kronicle.errors.error_types import BadRequestError, ConflictError, DatabaseInstructionError, NotFoundError
from kronicle.schemas.payload.op_feedback import OpFeedback
from kronicle.schemas.payload.processed_payload import ProcessedPayload
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.types.tag_type import TagType
from kronicle.utils.asyncpg_utils import table_exists
from kronicle.utils.dev_logs import log_d, log_e
from kronicle.utils.str_utils import (
    ensure_uuid4,
    normalize_name,
    normalize_pg_identifier,
    uuid_to_str,
)

mod = "chan_meta"


# --------------------------------------------------------------------------------------------------
# ChannelMetadata
# --------------------------------------------------------------------------------------------------
class ChannelMetadata(BaseModel):
    """
    Part of the payload that describes the data.
    One channel metadata identifies one peculiar channel stream.
    `channel_id` identifies uniquely the channel stream from which we collect data.
    `received_at` is an auto-generated datetime tag
        that stores the date the user requested the metadata to be created.
    `channel_schema` describes the columns schema, i.e. the types of the columns of data in
        Python-like normalized application types (aka SchemaType).
        It is immutable (once created, it will not be updated)
    Optional `metadata` and `tag` are user-defined fields that can be used to add further
        information on the data.
    `column_metadata` maps each table column to a structured, mutable `ColumnMetadata`
        (unit, description, ontology + any extra descriptors). Keys are the normalized
        DB column names (NOT user column names).

    """

    channel_id: UUID
    channel_schema: ChannelSchema
    name: str | None = None
    user_metadata: dict[str, Any] | None = Field(default_factory=dict)
    tags: dict[str, TagType] | None = Field(default_factory=dict)
    column_metadata: ColumnMetadataMap | None = Field(default_factory=dict)
    received_at: IsoDateTime = Field(default_factory=lambda: IsoDateTime.now_local())

    _NAMESPACE: ClassVar[str] = normalize_pg_identifier("data")
    _TABLE_NAME: ClassVar[str] = normalize_pg_identifier("channel_metadata")
    _TABLE_SCHEMA: ClassVar[dict[str, str]] = {
        "channel_id": "UUID PRIMARY KEY",
        "channel_schema": "JSONB NOT NULL",
        "name": "TEXT UNIQUE",
        "user_metadata": "JSONB",
        "tags": "JSONB",
        "column_metadata": "JSONB",
        "received_at": "TIMESTAMPTZ NOT NULL DEFAULT now()",
    }  # column names should always be in [a-z_]+ !!!

    _feedback: OpFeedback = PrivateAttr(default_factory=OpFeedback)

    @property
    def feedback(self) -> OpFeedback:
        return self._feedback

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
        try:
            return normalize_name(s, prefix="channel_")
        except ValueError:
            return None

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

    @model_validator(mode="after")
    def _normalize_column_metadata(self) -> ChannelMetadata:
        """
        Normalize `column_metadata`: coerce values to ColumnMetadata, drop empty
        entries, and reject keys that are not real columns of the channel schema.

        Raises:
            BadRequestError: if a key is not a known DB column.
        """
        if not self.column_metadata:
            self.column_metadata = {}
            return self
        self.column_metadata = normalize_column_metadata(
            self.column_metadata,
            allowed_columns=self.channel_schema.column_types.keys(),
        )
        return self

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
    def tablename(cls):
        return str(cls._TABLE_NAME)

    @classmethod
    def table(cls):
        """Fully-qualified table name; a constant (``data.channel_metadata``), safe to interpolate."""
        return f"{cls._NAMESPACE}.{cls._TABLE_NAME}"

    @classmethod
    def table_schema(cls) -> dict[str, str]:
        """Read-only access to the table schema dict."""
        return dict(cls._TABLE_SCHEMA)  # return a copy to avoid mutation

    @classmethod
    def get_schema_defs(cls) -> str:
        """Return SQL column definitions for CREATE TABLE."""
        return ", ".join(f"{normalize_pg_identifier(col)} {typ}" for col, typ in cls.table_schema().items())

    @classmethod
    def get_schema_columns(cls) -> list[tuple[str, str]]:
        """Return ordered list of (column, type) tuples."""
        return list(cls.table_schema().items())

    # ----------------------------------------------------------------------------------------------
    # Serialization
    # ----------------------------------------------------------------------------------------------
    def to_json(self) -> dict:
        meta = {
            "channel_id": uuid_to_str(self.channel_id),
            "channel_schema": self.channel_schema.to_user_json(),
            "name": self.name,
            "user_metadata": self.user_metadata,
            "tags": self.tags,
            "column_metadata": self.column_metadata_json() or None,
            "received_at": str(self.received_at),
        }
        return {k: v for k, v in meta.items() if v is not None}

    def __str__(self) -> str:
        return str(self.to_json())

    def column_metadata_json(self) -> dict[str, dict[str, Any]]:
        """JSON-serializable form of `column_metadata` (asyncpg/JSONB-ready)."""
        return {name: cm.to_json() for name, cm in (self.column_metadata or {}).items()}

    # ----------------------------------------------------------------------------------------------
    # DB row helpers
    # ----------------------------------------------------------------------------------------------
    def db_ready_values(self) -> list[Any]:
        """
        Return the values in a format ready to be inserted into PostgreSQL.
        JSONB fields are passed as dicts, not strings.
        """
        return [
            self.channel_id,
            self.channel_schema.to_user_json(),  # asyncpg will handle dict -> JSONB
            self.name or None,
            self.user_metadata or {},  # dict for JSONB
            self.tags or {},  # dict for JSONB
            self.column_metadata_json() or {},  # dict for JSONB
            self.received_at,
        ]

    @classmethod
    def from_db(cls, record: dict) -> ChannelMetadata:
        """Create ChannelMetadata from a DB row dict."""
        channel_id = record["channel_id"]
        name = record.get("name") or ""
        metadata = record.get("user_metadata") or {}
        tags = record.get("tags") or {}
        column_metadata = record.get("column_metadata") or {}
        received = record.get("received_at")
        received_iso = IsoDateTime.to_iso_datetime(received) if received is not None else IsoDateTime.now_local()
        channel_schema = ChannelSchema.from_user_json(
            loads(record["channel_schema"]) if isinstance(record["channel_schema"], str) else record["channel_schema"]
        )
        return cls(
            channel_id=channel_id,
            channel_schema=channel_schema,
            name=name,
            user_metadata=metadata if isinstance(metadata, dict) else loads(metadata),
            tags=tags if isinstance(tags, dict) else loads(tags),
            column_metadata=column_metadata if isinstance(column_metadata, dict) else loads(column_metadata),
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
        Handles normalization and validation of metadata, tags, name, schema and column_metadata.
        """
        channel_id = ensure_uuid4(processed.channel_id)
        if channel_truth:
            if processed.channel_schema:
                if channel_truth.equivalent_to(processed.channel_schema):
                    raise BadRequestError(
                        "This payload is not compatible with the existing channel schema",
                        details={
                            "channel_id": uuid_to_str(channel_id),
                            "channel_schema": {
                                "expected": channel_truth,
                                "received": processed.channel_schema,
                            },
                        },
                    )
            channel_schema = channel_truth
        else:
            channel_schema = processed.channel_schema
        name = normalize_name(processed.name, prefix="channel_") if processed.name else ""

        return cls(
            channel_id=channel_id,
            name=name,
            channel_schema=channel_schema,
            user_metadata=processed.metadata or {},
            tags=processed.tags or {},
            column_metadata=processed.column_metadata or {},
        )

    def update_with_db(self, record: Record):
        db_obj = type(self).from_db(dict(record))
        for field in type(self).model_fields:  # <-- class-level access!
            setattr(self, field, getattr(db_obj, field))
        log_d(mod, f"ChannelMetadata updated in place: {self.channel_id}")
        return self

    # ----------------------------------------------------------------------------------------------
    # DB operations: table
    # ----------------------------------------------------------------------------------------------
    @classmethod
    async def table_exists(cls, db: PoolConnectionProxy) -> bool:
        """Return True if the ChannelMetadata table exists."""
        return await table_exists(db, namespace=cls.namespace(), table_name=cls.tablename())

    @classmethod
    def create_table_sql(cls) -> str:
        return f"CREATE TABLE IF NOT EXISTS {cls.table()} ({cls.get_schema_defs()});"

    @classmethod
    async def ensure_table(cls, db: PoolConnectionProxy):
        """Ensure the table exists in the database."""
        if await cls.table_exists(db):
            return
        await db.execute(cls.create_table_sql())
        log_d("ensure_table", "Table created", cls.table())

    # ----------------------------------------------------------------------------------------------
    # DB operations: read/fetch
    # ----------------------------------------------------------------------------------------------
    async def exists(self, db: PoolConnectionProxy) -> bool:
        res = await self.fetch_by_id(db, self.channel_id)
        return res is not None

    @classmethod
    async def fetch_by_id(cls, db: PoolConnectionProxy, channel_id: UUID) -> ChannelMetadata | None:
        """
        Fetch a metadata row by channel_id.

        Args:
            db: asyncpg.Connection object
            channel_id: UUID of the channel
        Returns:
            ChannelMetadata instance or None if not found
        """
        sql = f"SELECT * FROM {cls.table()} WHERE channel_id = $1"  # constant table # nosec B608
        record = await db.fetchrow(sql, ensure_uuid4(channel_id))
        if not record:
            return None
        return cls.from_db(dict(record))

    @classmethod
    async def fetch_by_name(cls, db: PoolConnectionProxy, name: str) -> ChannelMetadata | None:
        """
        Fetch a metadata by name.

        Args:
            db: asyncpg.Connection object
            name: str, name of the channel (will be normalized)

        Returns:
            ChannelMetadata instance or None if not found
        """
        sql = f"SELECT * FROM {cls.table()} WHERE name = $1"  # constant table # nosec B608
        try:
            norm_name = normalize_name(name, prefix="channel_")
        except ValueError:
            return None
        record = await db.fetchrow(sql, norm_name)
        if not record:
            return None
        return cls.from_db(dict(record))

    @classmethod
    async def fetch_by_tags(cls, db: PoolConnectionProxy, tags: dict[str, str]) -> list[ChannelMetadata]:
        """
        Fetch every metadata that match all specified tags.

        Args:
            db: asyncpg.Connection object
            tags: dict (all key:value pairs of which must match)

        Returns:
            List of ChannelMetadata instances matching all tags
        """
        if not tags:
            return []

        sql = f"""
        SELECT * FROM {cls.table()}
        WHERE tags @> $1
        ORDER BY received_at DESC
        """  # constant table # nosec B608
        record_list = await db.fetch(sql, tags)
        return [cls.from_db(dict(r)) for r in record_list]

    @classmethod
    async def fetch_by_user_meta(cls, db: PoolConnectionProxy, user_meta: dict[str, str]) -> list[ChannelMetadata]:
        """
        Fetch every metadata that match all specified tags.

        Args:
            db: asyncpg.Connection object
            user_meta: dict (all key:value pairs of which must match)

        Returns:
            List of ChannelMetadata instances matching all input metadata
        """
        here = "get_by_user_meta"
        if not user_meta:
            return []

        sql = f"""
        SELECT * FROM {cls.table()}
        WHERE user_metadata @> $1
        ORDER BY received_at DESC
        """  # constant table # nosec B608
        log_d(here, "user_meta:", user_meta)
        record_list = await db.fetch(sql, user_meta)
        log_d(here, "record_list:", record_list)

        return [cls.from_db(dict(r)) for r in record_list]

    @classmethod
    async def fetch_all(cls, db: PoolConnectionProxy) -> list[ChannelMetadata]:
        """
        Fetch every metadata, ordered by received_at descending.

        Args:
            db: asyncpg.Connection object

        Returns:
            List of ChannelMetadata objects
        """
        sql = f"SELECT * FROM {cls.table()} ORDER BY received_at DESC"  # constant table # nosec B608
        record_list = await db.fetch(sql)
        return [cls.from_db(dict(r)) for r in record_list]

    # ----------------------------------------------------------------------------------------------
    # DB operations: create
    # ----------------------------------------------------------------------------------------------
    async def create(self, db: PoolConnectionProxy) -> ChannelMetadata:
        """
        Insert a new ChannelMetadata.
        Raises an error if the channel_id already exists.
        """
        # ChannelMetadata table is supposed to exist (checked at bootstrap)
        here = "create"
        # log_d(here)
        # Check if metadata exists
        if await self.exists(db):
            raise ConflictError("ChannelMetadata already exists", details={"channel_id": uuid_to_str(self.channel_id)})
        columns = list(self.table_schema().keys())
        placeholders = [f"${i + 1}" for i in range(len(columns))]
        sql = f"""
        INSERT INTO {self.table()} ({", ".join(columns)})
        VALUES ({", ".join(placeholders)})
        RETURNING *;
        """  # constant table; constant column names # nosec B608
        # log_d(here, "sql", sql)
        # log_d(here, "db_ready_values", self.db_ready_values())
        try:
            record = await db.fetchrow(sql, *self.db_ready_values())
            if record is None:
                log_e(here, f"INSERT did not return a record - channel_id={self.channel_id}")
                raise DatabaseInstructionError(
                    "INSERT did not return a record", details={"channel_id": uuid_to_str(self.channel_id)}
                )
            log_d(mod, f"ChannelMetadata created: {self.channel_id}")
            return self.update_with_db(record)
        except UniqueViolationError as e:
            # Determine which field caused the violation
            constraint = getattr(e, "constraint_name", "")  # asyncpg sets this if the DB reports the constraint name
            if constraint == f"{self.tablename()}_pkey":
                raise ConflictError(
                    "ChannelMetadata with this ID already exists", details={"channel_id": uuid_to_str(self.channel_id)}
                ) from e
            elif constraint == f"{self.tablename()}_name_key":
                raise ConflictError("ChannelMetadata with this name already exists", details={"name": self.name}) from e
            else:
                # fallback: include original error args
                raise ConflictError(
                    "ChannelMetadata violates a unique constraint",
                    details={"channel_id": uuid_to_str(self.channel_id), "name": self.name},
                ) from e

    # ----------------------------------------------------------------------------------------------
    # DB operations: update
    # ----------------------------------------------------------------------------------------------
    async def update(self, db: PoolConnectionProxy) -> ChannelMetadata:
        """
        Update mutable fields of an existing ChannelMetadata.
        Immutable fields: channel_id, channel_schema
        Mutable fields: name, user_metadata, tags, column_metadata
        """
        # ChannelMetadata table is supposed to exist (checked at bootstrap)

        # Fetch existing metadata
        existing = await self.fetch_by_id(db, self.channel_id)
        if not existing:
            raise ValueError(f"No ChannelMetadata found for id {self.channel_id}")

        updatable_columns = ["name", "user_metadata", "tags", "column_metadata"]
        placeholders = [f"${i + 1}" for i in range(len(updatable_columns) + 1)]  # +1 for WHERE

        sql = f"""
            UPDATE {self.table()}
            SET {", ".join(f"{col} = {placeholders[idx]}" for idx, col in enumerate(updatable_columns))}
            WHERE channel_id = {placeholders[-1]}
            RETURNING *;
            """  # constant table; constant column names # nosec B608
        values = []
        for col in updatable_columns:
            if col == "name":
                values.append(self.name)
            elif col == "column_metadata":
                values.append(self.column_metadata_json())
            else:
                values.append(getattr(self, col) or {})
        values.append(self.channel_id)

        try:
            record = await db.fetchrow(sql, *values)
            if record is None:
                raise NotFoundError("No ChannelMetadata found", details={"channel_id": uuid_to_str(self.channel_id)})
            log_d(mod, f"ChannelMetadata updated: {self.channel_id}")
            # Return a new instance built from the updated metadata
            return self.update_with_db(record)
        except UniqueViolationError as e:
            constraint = getattr(e, "constraint_name", "")
            if constraint == f"{self.tablename()}_name_key":
                raise ConflictError("Another channel with this name already exists", details={"name": self.name}) from e
            else:
                raise ConflictError(
                    "ChannelMetadata violates a unique constraint",
                    details={"channel_id": uuid_to_str(self.channel_id), "name": self.name},
                ) from e

    # ----------------------------------------------------------------------------------------------
    # DB operations: delete
    # ----------------------------------------------------------------------------------------------
    async def delete(self, db: PoolConnectionProxy) -> ChannelMetadata | None:
        """
        Delete this channel from the database.

        Returns:
            The deleted ChannelMetadata instance, or None if it didn't exist.
        """
        existing = await self.fetch_by_id(db, self.channel_id)
        if not existing:
            return None

        sql = f"""
        DELETE FROM {self.table()}
        WHERE channel_id = $1
        RETURNING *;
        """  # constant table # nosec B608
        record = await db.fetchrow(sql, self.channel_id)
        if not record:
            return None  # row didn't exist
        return self.update_with_db(record)


# --------------------------------------------------------------------------------------------------
# Main test
# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":  # pragma: no cover
    here = "channel_schema tests"
    log_d(here, "ChannelMetadata.namespace", ChannelMetadata.namespace())  # prints data => OK

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
        column_metadata={
            "temperature": {"unit": "Cel", "description": "Air temperature"},
            "humidity": {"unit": "%"},
        },  # type: ignore[reportArgumentType]  (raw dicts coerced by model validator)
    )

    log_d(here, "metadata", metadata)
    log_d(here, "column_metadata JSON", metadata.column_metadata_json())
    log_d(here, "DB ready values", metadata.db_ready_values())

    try:
        ChannelMetadata(
            channel_id=uuid4(),
            channel_schema=channel_schema,
            column_metadata={"not_a_column": {"unit": "C"}},  # type: ignore[reportArgumentType]
        )
    except BadRequestError as e:
        log_d(here, "unknown column rejected", e.message, e.details)

    sample_meta = {
        "time": IsoDateTime.now_local(),
        "temperature": 25.5,
        "humidity": 50.2,
        "meta": {"note": "ok"},
        "tags": [1, 2],
    }

    for col, col_type in ChannelMetadata.get_schema_columns():
        log_d(here, f"type of schema column `{col}`", col_type)
