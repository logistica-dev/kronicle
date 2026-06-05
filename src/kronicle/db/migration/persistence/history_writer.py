# kronicle/db/migration/persistence/history_writer.py
from __future__ import annotations

from sqlalchemy.orm import Session

from kronicle.db.migration.execution_result import MigrationExecutionResult


class HistoryWriter:
    """
    Persists per-operation migration history.
    """

    def __init__(self, session: Session, model):
        self.session = session
        self.model = model  # Core or RBAC model class

    def write(self, result: MigrationExecutionResult, applied_by: str):
        for op_result in result.operations:
            row = self.model(
                revision=result.revision,
                previous_revision=None,
                operation_type=op_result.operation.describe(),
                target=op_result.operation.describe(),
                plan_hash="TODO",  # can be added later from MigrationPlan
                applied_at=result.finished_at,
                applied_by=applied_by,
                safety_level=getattr(op_result.operation, "safety", "safe"),
                success=op_result.success,
                rollback_supported=False,
                operation_payload={
                    "error": op_result.error,
                    "duration_ms": op_result.duration_ms,
                },
            )
            self.session.add(row)

        self.session.flush()
