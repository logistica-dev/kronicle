# kronicle/db/migration/persistence/schema_migration_history.py
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kronicle.db.base.kronicle_base import KronicleBase
from kronicle.db.core.models.core_entity import CoreEntity
from kronicle.db.rbac.models.rbac_entity import RbacEntity


class SchemaMigrationHistory(KronicleBase):
    """
    Immutable audit log of executed schema migrations.

    One table per PostgreSQL schema (core / rbac).
    """

    __abstract__ = True

    revision: Mapped[str]
    previous_revision: Mapped[str | None]

    operation_type: Mapped[str]
    target: Mapped[str]

    plan_hash: Mapped[str]

    applied_at: Mapped[datetime]
    applied_by: Mapped[str]

    safety_level: Mapped[str]

    success: Mapped[bool]
    rollback_supported: Mapped[bool]

    operation_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)


# --------------------------------------------------------------------------------------------------
# CORE
# --------------------------------------------------------------------------------------------------


class CoreSchemaMigrationHistory(SchemaMigrationHistory):
    __tablename__ = "schema_migration_history"

    __table_args__ = (
        Index("ix_core_migration_history_revision", "revision"),
        Index("ix_core_migration_history_applied_at", "applied_at"),
        Index("ix_core_migration_history_revision_applied_at", "revision", "applied_at"),
        {"schema": CoreEntity.namespace(), "extend_existing": True},
    )


# --------------------------------------------------------------------------------------------------
# RBAC
# --------------------------------------------------------------------------------------------------


class RbacSchemaMigrationHistory(SchemaMigrationHistory):
    __tablename__ = "schema_migration_history"

    __table_args__ = (
        Index("ix_rbac_migration_history_revision", "revision"),
        Index("ix_rbac_migration_history_applied_at", "applied_at"),
        Index("ix_rbac_migration_history_revision_applied_at", "revision", "applied_at"),
        {"schema": RbacEntity.namespace(), "extend_existing": True},
    )
