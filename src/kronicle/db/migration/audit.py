from dataclasses import dataclass

from sqlalchemy.orm import DeclarativeMeta

from kronicle.db.base.kronicle_base import Base
from scripts.migrations.env import KronicleBase


@dataclass
class MigrationAuditSnapshot:
    table_count: int
    schemas: list[str]
    foreign_key_count: int
    table_names: list[str]


def collect_metadata_snapshot() -> MigrationAuditSnapshot:
    """
    Captures the state of SQLAlchemy metadata BEFORE migration runs.
    """

    tables = KronicleBase.metadata.tables.values()

    schemas = sorted({t.schema for t in tables if t.schema})
    table_names = sorted(Base.metadata.tables.keys())

    fk_count = sum(len(t.foreign_keys) for t in tables)

    return MigrationAuditSnapshot(
        table_count=len(Base.metadata.tables),
        schemas=schemas,
        foreign_key_count=fk_count,
        table_names=table_names,
    )


def log_snapshot(snapshot: MigrationAuditSnapshot, logger):
    """
    Emits structured migration diagnostics.
    """

    logger.info("=== Migration Audit Snapshot ===")
    logger.info(f"Tables: {snapshot.table_count}")
    logger.info(f"Schemas: {snapshot.schemas}")
    logger.info(f"Foreign keys: {snapshot.foreign_key_count}")
