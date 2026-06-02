# kronicle/db/migration/migration_manager.py
from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import select

from kronicle.db.base.kronicle_base import Base
from kronicle.db.migration.db_catalog import DatabaseCatalogBuilder
from kronicle.db.migration.migration_plan import MigrationPlan
from kronicle.db.migration.migration_proposal import MigrationProposal
from kronicle.db.migration.persistence.schema_migration_history import (
    CoreSchemaMigrationHistory,
    RbacSchemaMigrationHistory,
)
from kronicle.db.migration.persistence.schema_migration_state import (
    CoreSchemaMigrationState,
    RbacSchemaMigrationState,
)
from kronicle.db.rbac.rbac_db_session import RbacDbSession
from kronicle.db.registry import get_migration_schemas
from kronicle.deps.settings import KronicleSettings
from kronicle.deps.settings_env import KRONICLE_SQLA_BACKUP
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.utils.dev_logs import log_e, log_i, log_w

mod = "migration"

# --------------------------------------------------------------------------------------
# Schema → model mapping
# --------------------------------------------------------------------------------------
_SCHEMA_STATE: Dict[str, type] = {}
_SCHEMA_HISTORY: Dict[str, type] = {}


def _register_schema_models():
    if _SCHEMA_STATE:
        return
    from kronicle.db.core.models.core_entity import CoreEntity
    from kronicle.db.rbac.models.rbac_entity import RbacEntity

    _SCHEMA_STATE[CoreEntity.namespace()] = CoreSchemaMigrationState
    _SCHEMA_STATE[RbacEntity.namespace()] = RbacSchemaMigrationState
    _SCHEMA_HISTORY[CoreEntity.namespace()] = CoreSchemaMigrationHistory
    _SCHEMA_HISTORY[RbacEntity.namespace()] = RbacSchemaMigrationHistory


_register_schema_models()


# ======================================================================================
# MigrationManager
# ======================================================================================


class MigrationManager:
    """
    Orchestrates the full migration lifecycle:

        1. Pre-check (current state, drift detection)
        2. Proposal (diff)
        3. Plan (ordering + safety)
        4. User gate
        5. Backup
        6. Execution (Alembic Ops)
        7. History + State recording (chained revisions)
    """

    def __init__(self, db_url: str, alembic_cfg_path: str = "alembic.ini", *, auto_approve: bool = False):
        self.cfg = Config(alembic_cfg_path)
        self.db_url = db_url

        self.schemas = get_migration_schemas()
        self.db = RbacDbSession(db_url)
        self._previous_revisions: Dict[str, str | None] = {}

        self.auto_approve = auto_approve

        log_i(mod, f"Migration scope: {self.schemas}")

    # ------------------------------------------------------------------
    # BACKUP
    # ------------------------------------------------------------------

    def backup(self) -> Path:
        backup_prefix = os.environ.get(KRONICLE_SQLA_BACKUP, "./backups/kronicle")
        backup_prefix_path = Path(backup_prefix)

        ts = IsoDateTime.now_utc().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_prefix_path.parent / f"{backup_prefix_path.name}_{ts}.dump"
        backup_file.parent.mkdir(parents=True, exist_ok=True)

        cmd = ["pg_dump", "-Fc", "-f", str(backup_file), self.db_url]

        for schema in self.schemas:
            cmd += ["-n", schema]

        log_i(mod, f"Creating backup: {backup_file}")
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            log_e(mod, f"Backup failed: {e.stderr}")
            raise RuntimeError(f"Backup failed — aborting migration: {e.stderr}") from e

        return backup_file

    # ------------------------------------------------------------------
    # PRE-MIGRATION CHECK
    # ------------------------------------------------------------------

    def _compute_db_hash(self, conn, schema: str) -> str:
        builder = DatabaseCatalogBuilder(conn)
        return builder.from_database(schema).compute_hash()

    @staticmethod
    def _compute_metadata_hash(schema: str) -> str:
        tables = {n: t for n, t in Base.metadata.tables.items() if t.schema == schema}
        catalog = DatabaseCatalogBuilder.from_metadata(tables)
        return catalog.compute_hash()

    def _pre_migration_check(self, conn):
        """
        Read current stored state for each schema, compute actual DB hash,
        and detect drift.
        """
        for schema in self.schemas:
            state_cls = _SCHEMA_STATE[schema]
            history_cls = _SCHEMA_HISTORY[schema]

            # Ensure tracking tables exist
            state_cls.ensure_table(conn)
            history_cls.ensure_table(conn)

            # Actual DB hash
            actual_hash = self._compute_db_hash(conn, schema)

            # Stored state
            row = conn.execute(select(state_cls).order_by(state_cls.created_at.desc()).limit(1)).first()

            if row is not None:
                self._previous_revisions[schema] = row.revision
                if row.schema_hash != actual_hash:
                    log_w(
                        mod,
                        f"Schema '{schema}' has drifted from recorded state "
                        f"(stored hash: {row.schema_hash[:12]}… ≠ actual: {actual_hash[:12]}…)",
                    )
                else:
                    log_i(mod, f"Schema '{schema}' state matches recorded hash")
            else:
                self._previous_revisions[schema] = None
                log_i(mod, f"Schema '{schema}' has no prior migration state — first migration")

            # Check if metadata already matches (nothing to do)
            metadata_hash = self._compute_metadata_hash(schema)
            if actual_hash == metadata_hash:
                log_i(mod, f"Schema '{schema}' already up to date")

    # ------------------------------------------------------------------
    # CORE PIPELINE
    # ------------------------------------------------------------------

    def build_plan(self) -> MigrationPlan:
        """Proposal → Plan."""
        with self.db._engine.connect() as conn:
            proposal = MigrationProposal(conn)
            ops = proposal.to_operations()
            return MigrationPlan.build(ops)

    # ------------------------------------------------------------------
    # EXECUTION
    # ------------------------------------------------------------------

    def apply_plan(self, plan: MigrationPlan) -> None:
        """Execute migration plan via Alembic Operations context."""
        log_i(mod, f"Executing migration plan: {len(plan.ordered_operations)} ops")

        with self.db._engine.begin() as conn:
            context = MigrationContext.configure(conn)
            ops = Operations(context)
            plan.apply(ops)

    # ------------------------------------------------------------------
    # STATE RECORDING
    # ------------------------------------------------------------------

    def record_migration_state(
        self,
        plan: MigrationPlan,
        *,
        success: bool,
        applied_by: str = "system",
    ):
        """
        Persist migration outcome into schema_migration_history
        and schema_migration_state for each affected schema.

        History stores one row per operation for full introspection.
        State stores the final schema hash after the plan.
        Revisions are chained via previous_revision.
        """
        now = datetime.now(timezone.utc)

        for schema in self.schemas:
            state_cls = _SCHEMA_STATE[schema]
            history_cls = _SCHEMA_HISTORY[schema]

            metadata_hash = self._compute_metadata_hash(schema)
            previous_revision = self._previous_revisions.get(schema)

            with self.db._engine.begin() as conn:
                # --- History: one row per operation ---
                for idx, op in enumerate(plan.ordered_operations):
                    op_desc = op.describe()
                    conn.execute(
                        history_cls.__table__.insert().values(
                            revision=plan.revision,
                            previous_revision=previous_revision,
                            operation_index=idx,
                            operation_type=op_desc.split(":")[0],
                            target=op_desc,
                            plan_hash=plan.revision,
                            applied_at=now,
                            applied_by=applied_by,
                            safety_level=op.safety.level,
                            success=success,
                            rollback_supported=False,
                            operation_payload={
                                "metadata_hash": metadata_hash,
                                "schema": schema,
                            },
                            details={},
                        )
                    )

                # --- State entry (one per schema per migration) ---
                catalog = DatabaseCatalogBuilder.from_metadata(
                    {n: t for n, t in Base.metadata.tables.items() if t.schema == schema}
                )
                conn.execute(
                    state_cls.__table__.insert().values(
                        revision=plan.revision,
                        schema_hash=metadata_hash,
                        applied_at=now,
                        applied_by=applied_by,
                        operation_count=len(plan.ordered_operations),
                        metadata_snapshot={
                            "namespace": schema,
                            "tables": {
                                tc.name: [
                                    {"name": cc.name, "type": cc.type, "nullable": cc.nullable} for cc in tc.columns
                                ]
                                for tc in catalog.tables
                            },
                        },
                        details={},
                    )
                )

        if success:
            log_i(mod, f"Migration recorded — revision {plan.revision}")
        else:
            log_w(mod, f"Failed migration recorded — revision {plan.revision}")

    # ------------------------------------------------------------------
    # RUN
    # ------------------------------------------------------------------

    def run(self, verbose: bool = True):
        log_i(mod, "Starting migration pipeline")

        plan = None
        backup_file = None

        try:
            # --------------------------------------------------
            # 1. PRE-CHECK (read stored state, detect drift)
            # --------------------------------------------------
            with self.db._engine.connect() as conn:
                self._pre_migration_check(conn)

            # --------------------------------------------------
            # 2. BUILD PLAN (no side effects)
            # --------------------------------------------------
            plan = self.build_plan()

            if not plan.operations:
                log_i(mod, "No changes detected — nothing to apply")
                return

            if verbose:
                log_i(mod, f"Plan summary: {plan.summary()}")

            # --------------------------------------------------
            # 3. USER GATE (before ANY mutation)
            # --------------------------------------------------
            if not self.auto_approve:
                confirm = input("Review above migration plan.\n" "Proceed with backup + migration? (y/n): ")
                if confirm.lower() != "y":
                    log_i(mod, "Migration aborted by user")
                    return

            # --------------------------------------------------
            # 4. SIDE EFFECTS (only after approval)
            # --------------------------------------------------
            backup_file = self.backup()

            self.apply_plan(plan)
            self.record_migration_state(plan, success=True)

        except Exception as e:
            log_i(mod, f"FAILED migration: {e}")

            if backup_file:
                log_i(mod, f"Restoring backup: {backup_file}")
                subprocess.run(
                    ["pg_restore", "-d", self.db_url, str(backup_file)],
                    check=True,
                )

            if plan is not None:
                self.record_migration_state(plan, success=False)

            raise

        log_i(mod, "Migration completed successfully")


# ======================================================================================
# CLI entrypoint
# ======================================================================================


if __name__ == "__main__":

    settings = KronicleSettings()
    db_url = settings.db.rbac_connection_url

    manager = MigrationManager(db_url=db_url)
    manager.run()
