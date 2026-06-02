# kronicle/db/migration/migration_proposal.py
from __future__ import annotations

from sqlalchemy.engine import Connection

from kronicle.db.base.kronicle_table import Base
from kronicle.db.migration.operations import DbStructureOperation, SafetyLevel
from kronicle.db.migration.schema_diff_engine import SchemaDiffEngine
from kronicle.db.registry import get_migration_schemas
from kronicle.utils.dev_logs import log_e, log_i, log_w

mod = "migration_proposal"


class MigrationProposal:
    """
    Diff engine: delegates to SchemaDiffEngine for full column-level diff
    (types, nullability, schemas, tables, columns).
    """

    def __init__(self, connection: Connection):
        self.connection = connection
        self.schemas = get_migration_schemas()

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    def _log_operation(self, op: DbStructureOperation):
        msg = f"{op.safety.level[:4].upper()} -> {op.describe()}"

        if op.safety.level == SafetyLevel.SAFE:
            log_i("migration", msg)
        elif op.safety.level == SafetyLevel.WARNING:
            log_w("migration", msg)
        else:
            log_e("migration", msg)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def to_operations(self) -> list[DbStructureOperation]:
        engine = SchemaDiffEngine(
            connection=self.connection,
            metadata=Base.metadata,
            schemas=self.schemas,
        )
        diff = engine.diff()
        ops = diff.operations

        for op in ops:
            self._log_operation(op)

        return ops
