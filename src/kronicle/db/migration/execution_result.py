# kronicle/db/migration/execution_result.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from kronicle.db.migration.operations import DbStructureOperation


@dataclass(frozen=True)
class OperationResult:
    """
    Result of a single operation execution.
    """

    operation: DbStructureOperation
    success: bool
    error: Optional[str] = None
    executed_at: datetime = field(default_factory=datetime.utcnow)
    duration_ms: float = 0.0


@dataclass(frozen=True)
class MigrationExecutionResult:
    """
    Full result of executing a MigrationPlan.

    This is what gets persisted into:
    - schema_migration_history (per operation)
    - schema_migration_state (final snapshot)
    """

    revision: str
    schema: str  # "core" or "rbac"

    started_at: datetime
    finished_at: datetime

    success: bool

    operations: List[OperationResult] = field(default_factory=list)

    backup_file: Optional[str] = None

    @property
    def failed_operations(self) -> List[OperationResult]:
        return [op for op in self.operations if not op.success]

    @property
    def operation_count(self) -> int:
        return len(self.operations)

    @property
    def duration_ms(self) -> float:
        return (self.finished_at - self.started_at).total_seconds() * 1000
