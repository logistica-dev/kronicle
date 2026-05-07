# kronicle/db/migration/persistence/schema_migration_state.py
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kronicle.db.base.kronicle_base import KronicleBase
from kronicle.db.core.models.core_entity import CoreEntity
from kronicle.db.rbac.models.rbac_entity import RbacEntity


class SchemaMigrationState(KronicleBase):
    """
    Current known state of a PostgreSQL schema.

    Exactly one row per schema table.
    """

    __abstract__ = True

    revision: Mapped[str]
    schema_hash: Mapped[str]

    applied_at: Mapped[datetime]
    applied_by: Mapped[str]

    operation_count: Mapped[int]

    metadata_snapshot: Mapped[dict] = mapped_column(JSONB, nullable=False)


# --------------------------------------------------------------------------------------------------
# CORE
# --------------------------------------------------------------------------------------------------


class CoreSchemaMigrationState(SchemaMigrationState):
    __tablename__ = "schema_migration_state"

    __table_args__ = (
        Index("ix_core_migration_state_revision", "revision"),
        Index("ix_core_migration_state_applied_at", "applied_at"),
        Index("ix_core_migration_state_revision_applied_at", "revision", "applied_at"),
        {"schema": CoreEntity.namespace(), "extend_existing": True},
    )


# --------------------------------------------------------------------------------------------------
# RBAC
# --------------------------------------------------------------------------------------------------


class RbacSchemaMigrationState(SchemaMigrationState):
    __tablename__ = "schema_migration_state"

    __table_args__ = (
        Index("ix_rbac_migration_state_revision", "revision"),
        Index("ix_rbac_migration_state_applied_at", "applied_at"),
        Index("ix_rbac_migration_state_revision_applied_at", "revision", "applied_at"),
        {"schema": RbacEntity.namespace(), "extend_existing": True},
    )
