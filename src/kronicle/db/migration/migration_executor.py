# kronicle/db/migration/migration_executor.py

from __future__ import annotations

from datetime import datetime
from typing import List

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.engine import Connection

from kronicle.db.migration.execution_result import (
    MigrationExecutionResult,
    OperationResult,
)
from kronicle.db.migration.migration_plan import MigrationPlan
from kronicle.db.migration.operations import DbStructureOperation
from kronicle.utils.dev_logs import log_e, log_i


class MigrationExecutor:
    """
    Executes a MigrationPlan against a live database connection.

    Responsibilities:
    - translate plan → Alembic Operations
    - execute operations sequentially
    - capture results
    - stop or continue on failure (configurable later)
    """

    def __init__(self, connection: Connection, *, strict: bool = True):
        self.connection = connection
        self.strict = strict

    # ------------------------------------------------------------------
    # PUBLIC ENTRY POINT
    # ------------------------------------------------------------------
    def execute(
        self,
        plan: MigrationPlan,
        *,
        revision: str,
        schema: str,
        applied_by: str = "system",
    ) -> MigrationExecutionResult:
        started_at = datetime.utcnow()
        log_i("migration_executor", f"Executing plan {revision} for schema={schema}")

        # Alembic operation context
        context = MigrationContext.configure(self.connection)
        op = Operations(context)

        results: List[OperationResult] = []
        success = True

        for operation in plan.ordered_operations:
            result = self._execute_operation(op, operation)
            results.append(result)

            # failure handling
            if not result.success:
                success = False
                log_e(
                    "migration_executor",
                    f"Operation failed: {operation.describe()} :: {result.error}",
                )

                if self.strict:
                    break

        finished_at = datetime.utcnow()

        return MigrationExecutionResult(
            revision=revision,
            schema=schema,
            started_at=started_at,
            finished_at=finished_at,
            success=success,
            operations=results,
        )

    # ------------------------------------------------------------------
    # SINGLE OPERATION EXECUTION
    # ------------------------------------------------------------------
    def _execute_operation(
        self,
        op: Operations,
        operation: DbStructureOperation,
    ) -> OperationResult:
        start = datetime.utcnow()

        try:
            log_i("migration_executor", f"Applying: {operation.describe()}")

            # actual execution
            operation.apply(op)

            end = datetime.utcnow()

            return OperationResult(
                operation=operation,
                success=True,
                duration_ms=(end - start).total_seconds() * 1000,
            )

        except Exception as e:
            end = datetime.utcnow()

            return OperationResult(
                operation=operation,
                success=False,
                error=str(e),
                duration_ms=(end - start).total_seconds() * 1000,
            )
