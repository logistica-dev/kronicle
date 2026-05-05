from dataclasses import dataclass

from kronicle.db.base.kronicle_base import Base
from kronicle.utils.dev_logs import log_i


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

    tables = Base.metadata.tables.values()

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
    here = "migration_audit"
    log_i(here, "=== Migration Audit Snapshot ===")
    log_i(here, f"Tables: {snapshot.table_count}")
    log_i(here, f"Schemas: {snapshot.schemas}")
    log_i(here, f"Foreign keys: {snapshot.foreign_key_count}")
