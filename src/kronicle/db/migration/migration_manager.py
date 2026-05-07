# kronicle/db/migration/migration_manager.py
from __future__ import annotations

import subprocess
from pathlib import Path

from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations

from kronicle.db.migration.migration_plan import MigrationPlan
from kronicle.db.migration.migration_proposal import MigrationProposal
from kronicle.db.rbac.rbac_db_session import RbacDbSession
from kronicle.db.registry import get_migration_schemas
from kronicle.deps.settings import KronicleSettings
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.utils.dev_logs import log_i

# ======================================================================================
# MigrationManager
# ======================================================================================


class MigrationManager:
    """
    Orchestrates the full migration lifecycle:

        1. Backup
        2. Proposal (diff)
        3. Plan (ordering + safety)
        4. Execution (Alembic Ops)
        5. History recording (future integration)
    """

    def __init__(self, db_url: str, alembic_cfg_path: str = "alembic.ini", *, auto_approve: bool = False):
        self.cfg = Config(alembic_cfg_path)
        self.db_url = db_url

        self.schemas = get_migration_schemas()
        self.db = RbacDbSession(db_url)

        self.auto_approve = auto_approve

        log_i("migration", f"Migration scope: {self.schemas}")

    # ------------------------------------------------------------------
    # BACKUP
    # ------------------------------------------------------------------

    def backup(self) -> Path:
        ts = IsoDateTime.now_utc().strftime("%Y%m%d_%H%M%S")
        backup_file = Path(f"./backups/kronicle_{ts}.dump")
        backup_file.parent.mkdir(exist_ok=True)

        cmd = ["pg_dump", "-Fc", "-f", str(backup_file), self.db_url]

        for schema in self.schemas:
            cmd += ["-n", schema]

        log_i("migration", f"Creating backup: {backup_file}")
        subprocess.run(cmd, check=True)

        return backup_file

    # ------------------------------------------------------------------
    # CORE PIPELINE
    # ------------------------------------------------------------------

    def build_plan(self):
        """
        Step 1–3:
        Proposal → Plan
        """
        with self.db._engine.connect() as conn:
            proposal = MigrationProposal(conn)

            ops = proposal.to_operations()
            plan = MigrationPlan.build(ops)

        return plan

    # ------------------------------------------------------------------
    # EXECUTION
    # ------------------------------------------------------------------

    def apply_plan(self, plan: MigrationPlan) -> None:
        """
        Execute migration plan via Alembic Operations context.
        """

        log_i("migration", f"Executing migration plan: {len(plan.ordered_operations)} ops")

        with self.db._engine.begin() as conn:
            context = MigrationContext.configure(conn)
            ops = Operations(context)

            plan.apply(ops)

    # ------------------------------------------------------------------
    # HISTORY (placeholder hook)
    # ------------------------------------------------------------------

    def record_migration_state(self, plan: MigrationPlan, success: bool):
        """
        Hook for:
        - schema_migration_state
        - schema_migration_history

        Keep isolated so RBAC/Core separation remains clean.
        """
        log_i("migration", f"Recording migration state (success={success})")
        # TODO: insert into:
        # - CoreSchemaMigrationHistory / RbacSchemaMigrationHistory
        # - CoreSchemaMigrationState / RbacSchemaMigrationState

    # ------------------------------------------------------------------
    # RUN
    # ------------------------------------------------------------------
    def run(self, verbose: bool = True):
        log_i("migration", "Starting migration pipeline")

        plan = None
        backup_file = None

        try:
            # --------------------------------------------------
            # 1. PURE: build plan (no side effects)
            # --------------------------------------------------
            plan = self.build_plan()

            if not plan.operations:
                log_i("migration", "No changes detected — nothing to apply")
                return

            if verbose:
                log_i("migration", f"Plan summary: {plan.summary()}")

            # --------------------------------------------------
            # 2. USER GATE (before ANY mutation)
            # --------------------------------------------------
            if not self.auto_approve:
                confirm = input("Review above migration plan.\n" "Proceed with backup + migration? (y/n): ")

                if confirm.lower() != "y":
                    log_i("migration", "Migration aborted by user")
                    return

            # --------------------------------------------------
            # 3. SIDE EFFECTS (only after approval)
            # --------------------------------------------------
            backup_file = self.backup()

            self.apply_plan(plan)
            self.record_migration_state(plan, success=True)

        except Exception as e:
            log_i("migration", f"FAILED migration: {e}")

            if backup_file:
                log_i("migration", f"Restoring backup: {backup_file}")
                subprocess.run(
                    ["pg_restore", "-d", self.db_url, str(backup_file)],
                    check=True,
                )

            if plan is not None:
                self.record_migration_state(plan, success=False)

            raise

        log_i("migration", "Migration completed successfully")


# ======================================================================================
# CLI entrypoint
# ======================================================================================


if __name__ == "__main__":
    settings = KronicleSettings()
    db_url = settings.db.rbac_connection_url

    manager = MigrationManager(db_url=db_url)
    manager.run()
