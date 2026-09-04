# kronicle/db/migration/db_rbac_provisioner.py
"""
RbacSchemasProvisioner: schema-level migration for the `core` and `rbac` schemas.

Complements DbProvisioner (infra: DB + owner roles + schemas + TimescaleDB extension).
This object owns the migration lifecycle for the two SQLAlchemy-backed, RBAC-owned
schemas (`core` + `rbac`): analysis, plan, user gate, backup, apply, state recording,
and the per-schema tracking tables (schema_migration_state / schema_migration_history).

Pipeline (check → plan → validate → execute), same principle as MigrationManager:
  1. check_analysis_requirements  — rbac owner connection can read the schemas
  2. check_tracking_tables_exist  — state + history tables present per schema
  3. pre_migration_check          — read stored state, detect drift (read-only)
  4. build_plan                   — diff metadata vs DB, no side effects
  5. check_mutation_requirements  — schemas owned (else dbsu ownership transfer), backup writable
  6. user gate                    — y/n before any mutation (skipped if auto_approve)
  7. execute                      — backup, clean orphans, apply plan (as owner), record state

Most of the migration-core here is copied from MigrationManager, scoped to core+rbac
(it also carried the `data` schema). MigrationManager is kept as a fallback/backup until
it is retired; the DataMigrationManager (asyncpg) will take over `data`.
"""

from __future__ import annotations

import argparse
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from json import dumps
from pathlib import Path

from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect, select, text

from kronicle.db.base.kronicle_base import Base
from kronicle.db.core.models._registry import CORE_NAMESPACE
from kronicle.db.migration.engine.db_catalog import DatabaseCatalogBuilder
from kronicle.db.migration.engine.migration_plan import MigrationPlan
from kronicle.db.migration.engine.migration_proposal import MigrationProposal
from kronicle.db.migration.engine.operations import AddColumnOp, SafetyLevel
from kronicle.db.migration.orchestrators.provisioner_base import BaseProvisioner, backup_path
from kronicle.db.migration.persistence.schema_migration_history import (
    CoreSchemaMigrationHistory,
    RbacSchemaMigrationHistory,
)
from kronicle.db.migration.persistence.schema_migration_state import (
    CoreSchemaMigrationState,
    RbacSchemaMigrationState,
)
from kronicle.db.rbac.models._registry import RBAC_NAMESPACE
from kronicle.db.rbac.rbac_db_session import RbacDbSession
from kronicle.db.registry import get_migration_schemas
from kronicle.deps.settings import KronicleSettings
from kronicle.deps.settings_env import KRONICLE_RBAC_BACKUP, DBSettings
from kronicle.utils.dev_logs import log_d, log_e, log_i, log_w
from kronicle.utils.file_utils import load_env_file

mod = "rbac_schemas"

# --------------------------------------------------------------------------------------
# Schema → model mapping (core + rbac only)
# --------------------------------------------------------------------------------------
_SCHEMA_STATE: dict[str, type] = {}
_SCHEMA_HISTORY: dict[str, type] = {}


def _register_schema_models():
    if _SCHEMA_STATE:
        return

    _SCHEMA_STATE[CORE_NAMESPACE] = CoreSchemaMigrationState
    _SCHEMA_STATE[RBAC_NAMESPACE] = RbacSchemaMigrationState
    _SCHEMA_HISTORY[CORE_NAMESPACE] = CoreSchemaMigrationHistory
    _SCHEMA_HISTORY[RBAC_NAMESPACE] = RbacSchemaMigrationHistory


_register_schema_models()


@dataclass
class _FkCheck:
    src_schema: str
    src_table: str
    ref_schema: str
    ref_table: str
    src_cols: list[str]
    ref_cols: list[str]


def _build_fk_checks(schemas: list[str], plan: MigrationPlan) -> list[_FkCheck]:
    """Collect FK relationships from metadata, skipping columns the plan will add."""

    adding_columns = {(op.schema, op.table, op.column_name) for op in plan.operations if isinstance(op, AddColumnOp)}

    checks: list[_FkCheck] = []
    for schema in schemas:
        tables = {n: t for n, t in Base.metadata.tables.items() if t.schema == schema}
        for table in tables.values():
            for fkc in table.foreign_key_constraints:
                src_cols = [col.name for col in fkc.columns]
                ref_elem = fkc.elements[0]
                ref_schema = ref_elem.column.table.schema or schema
                ref_table = ref_elem.column.table.name
                ref_cols = [elem.column.name for elem in fkc.elements]

                if any((schema, table.name, c) in adding_columns for c in src_cols):
                    continue

                checks.append(_FkCheck(schema, table.name, ref_schema, ref_table, src_cols, ref_cols))
    return checks


def _delete_orphans(checks: list[_FkCheck], conn_str: str) -> None:
    """Delete orphan rows for the given FK checks, converging in up to 10 passes."""
    for _ in range(10):
        total = 0
        for c in checks:
            sql = _build_orphan_delete_sql(c)
            result = subprocess.run(
                ["psql", "-d", conn_str, "-c", sql],
                check=True,
                capture_output=True,
                text=True,
            )
            for line in result.stdout.strip().splitlines():
                if line.startswith("DELETE "):
                    try:
                        total += int(line.split()[1])
                    except (IndexError, ValueError):
                        pass
        if total == 0:
            break
        log_i(mod, f"  Deleted {total} orphan row(s)")
    else:
        log_w(mod, "  Orphan cleanup did not converge after 10 passes")


def _build_orphan_delete_sql(c: _FkCheck) -> str:
    """Build a DELETE query that removes rows referencing non-existent parent rows."""
    where_parts = [
        f"NOT EXISTS (SELECT 1 FROM {c.ref_schema}.{c.ref_table} AS ref WHERE src.{src} = ref.{ref})"
        for src, ref in zip(c.src_cols, c.ref_cols, strict=True)
    ]
    return (
        f"DELETE FROM {c.src_schema}.{c.src_table} AS src"
        f" WHERE {' OR '.join(f'src.{col} IS NOT NULL' for col in c.src_cols)}"
        f" AND ({' AND '.join(where_parts)})"
    )


# --------------------------------------------------------------------------------------
# Prerequisite action labels (plan-readable, shown to the operator)
# --------------------------------------------------------------------------------------
_USER_CREATION = "creation of user"
_SCHEMA_OWNERSHIP = "ownership of schema"
_TRACK_TABLES_CREATION = "creation of the tracking tables for schema"


class RbacSchemasProvisioner(BaseProvisioner):
    """
    Orchestrates the migration lifecycle for the `core` and `rbac` schemas.

        1. Analysis requirements (owner can read schemas)
        2. Proposal (diff)
        3. Plan (ordering + safety)
        4. User gate (before any mutation)
        5. Backup
        6. Execution (Alembic Ops)
        7. History + State recording (chained revisions)

    Implements the BaseProvisioner contract (analyze / ask_validation / backup /
    restore_backup / execute_plan / run_post_analysis) driven by the shared
    ``run_once()`` workflow.
    """

    def __init__(
        self,
        db_settings: DBSettings,
        alembic_cfg_path: str = "alembic.ini",
        *,
        auto_approve: bool = False,
        backup_url: str | None = None,
    ):
        self.cfg = Config(alembic_cfg_path)
        self._db_settings = db_settings
        self.backup_url = backup_url or self.rbac_url

        self.schemas = sorted(get_migration_schemas())
        self._previous_revisions: dict[str, str | None] = {}

        self.rbac_db = RbacDbSession(self.rbac_url)

        self.auto_approve = auto_approve

        # Only `core` + `rbac`, both owned by the rbac user.
        self._schema_owners = {
            CORE_NAMESPACE: self.rbac_user,
            RBAC_NAMESPACE: self.rbac_user,
        }

        self._schema_connections = {
            CORE_NAMESPACE: (self.rbac_user, self.rbac_url),
            RBAC_NAMESPACE: (self.rbac_user, self.rbac_url),
        }

        # Owner passwords, used only to (re)create a missing role via the dbsu superuser.
        self._owner_passwords = {
            self.rbac_user: self._db_settings._rbac_pwd.get_secret_value(),
        }

        log_i(mod, f"Migration scope: {self.schemas}")

    @property
    def rbac_user(self) -> str:
        return self._db_settings._rbac_usr

    @property
    def rbac_url(self) -> str:
        return self._db_settings.rbac_connection_url

    @property
    def dbsu_url(self) -> str | None:
        return self._db_settings.dbsu_connection_url

    @property
    def schema_owners(self) -> dict:
        return self._schema_owners

    @property
    def schema_connections(self) -> dict:
        return self._schema_connections

    # ------------------------------------------------------------------
    # BACKUP
    # ------------------------------------------------------------------

    def _backup_connection_url(self) -> str:
        return self.dbsu_url or self.backup_url

    # ------------------------------------------------------------------
    # PRE-MIGRATION CHECK (read stored state, detect drift)
    # ------------------------------------------------------------------

    @staticmethod
    def _sync_tracking_table(conn, model_cls: type) -> None:
        """Add any columns the model declares but the DB table is missing."""
        schema_name = model_cls.__table__.schema
        table_name = model_cls.__tablename__
        inspector = inspect(conn)
        actual_cols = {c["name"] for c in inspector.get_columns(table_name, schema=schema_name)}
        declared_cols = set(model_cls.__table__.columns.keys())
        missing = declared_cols - actual_cols
        for col_name in sorted(missing):
            col = model_cls.__table__.columns[col_name]
            col_type = col.type.compile(conn.dialect)
            nullable = "NULL" if col.nullable else "NOT NULL"
            sql = f'ALTER TABLE {schema_name}.{table_name} ADD COLUMN "{col_name}" {col_type} {nullable}'
            log_i(mod, f"Syncing tracking table: adding missing column {schema_name}.{table_name}.{col_name}")
            conn.execute(text(sql))

    def _compute_db_hash(self, conn, schema: str) -> str:
        builder = DatabaseCatalogBuilder(conn)
        return builder.from_database(schema).compute_hash()

    @staticmethod
    def _compute_metadata_hash(schema: str) -> str:
        tables = {n: t for n, t in Base.metadata.tables.items() if t.schema == schema}
        catalog = DatabaseCatalogBuilder.from_metadata(tables)
        return catalog.compute_hash()

    def check_tracking_tables_exist(self, schema: str, url: str) -> bool:
        """Read-only: does the given schema have both tracking tables (state + history)?"""
        state_cls = _SCHEMA_STATE.get(schema)
        history_cls = _SCHEMA_HISTORY.get(schema)
        if state_cls is None or history_cls is None:
            return True  # no tracking tables defined for this schema
        engine = create_engine(url)
        try:
            with engine.connect() as conn:
                for table_name in (state_cls.__tablename__, history_cls.__tablename__):
                    row = conn.execute(
                        text(
                            "SELECT 1 FROM information_schema.tables "
                            "WHERE table_schema = :schema AND table_name = :name"
                        ),
                        {"schema": schema, "name": table_name},
                    ).first()
                    if row is None:
                        return False
                return True
        finally:
            engine.dispose()

    def ensure_tracking_tables(self, conn) -> None:
        """
        Create + sync the per-schema migration tracking tables.

        Must run inside a committed transaction: the plan only proposes them if
        they are already absent, and the failure path relies on them existing
        after the plan transaction rolls back / the restore replaces the schemas.
        """
        for schema in self.schemas:
            state_cls = _SCHEMA_STATE[schema]
            history_cls = _SCHEMA_HISTORY[schema]
            state_cls.ensure_table(conn)
            self._sync_tracking_table(conn, state_cls)
            history_cls.ensure_table(conn)
            self._sync_tracking_table(conn, history_cls)

    def pre_migration_check(self, conn):
        """
        Read current stored state for each schema, compute actual DB hash,
        and detect drift. Read-only: tracking tables are guaranteed beforehand.
        """
        log_i(mod, "Launching pre-migration checks")
        for schema in self.schemas:
            state_cls = _SCHEMA_STATE[schema]

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
                    metadata_hash = self._compute_metadata_hash(schema)
                    if metadata_hash == actual_hash:
                        log_d(mod, "  metadata == actual — diff engine agrees, hash mismatch is false positive")
                    else:
                        log_d(mod, f"  metadata ({metadata_hash[:12]}…) ≠ actual — real structural difference")

                        db_cat = DatabaseCatalogBuilder(conn).from_database(schema)
                        meta_tables = {n: t for n, t in Base.metadata.tables.items() if t.schema == schema}
                        meta_cat = DatabaseCatalogBuilder.from_metadata(meta_tables)
                        db_raw = dumps(db_cat.as_tuple(), default=str)
                        meta_raw = dumps(meta_cat.as_tuple(), default=str)
                        log_d(mod, f"  DB catalog len={len(db_raw)}  Meta catalog len={len(meta_raw)}")
                        log_d(mod, f"  DB catalog:   {db_raw[:2000]}")
                        log_d(mod, f"  Meta catalog: {meta_raw[:2000]}")
                        if db_raw == meta_raw:
                            log_d(mod, "  Catalogs are IDENTICAL — bug in compute_hash")
                        else:
                            for i, (a, b) in enumerate(zip(db_raw, meta_raw, strict=True)):
                                if a != b:
                                    log_d(mod, f"  First diff at pos {i}: DB={a!r} Meta={b!r}")
                                    break
                            if len(db_raw) != len(meta_raw):
                                log_d(mod, f"  Lengths differ: DB={len(db_raw)} Meta={len(meta_raw)}")
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
    # BUILD PLAN (no side effects)
    # ------------------------------------------------------------------

    def build_plan(self) -> MigrationPlan:
        """Proposal → Plan."""
        with self.rbac_db._engine.connect() as conn:
            proposal = MigrationProposal(conn)
            ops = proposal.to_operations()
            return MigrationPlan.build(ops)

    # ------------------------------------------------------------------
    # EXECUTION
    # ------------------------------------------------------------------

    def apply_plan(self, plan: MigrationPlan, connection=None) -> None:
        """Execute migration plan via Alembic Operations context."""
        log_i(mod, f"Executing migration plan: {len(plan.ordered_operations)} ops")

        if connection is not None:
            context = MigrationContext.configure(connection)
            ops = Operations(context)
            plan.apply(ops)
        else:
            with self.rbac_db._engine.begin() as conn:
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

        # Tracking tables may be missing on the failure path: they were rolled
        # back with the plan transaction and absent from the restored dump.
        with self.rbac_db._engine.begin() as conn:
            self.ensure_tracking_tables(conn)

        for schema in self.schemas:
            state_cls = _SCHEMA_STATE[schema]
            history_cls = _SCHEMA_HISTORY[schema]

            metadata_hash = self._compute_metadata_hash(schema)
            previous_revision = self._previous_revisions.get(schema)

            with self.rbac_db._engine.begin() as conn:
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
                actual_hash = self._compute_db_hash(conn, schema)
                catalog = DatabaseCatalogBuilder.from_metadata(
                    {n: t for n, t in Base.metadata.tables.items() if t.schema == schema}
                )
                conn.execute(
                    state_cls.__table__.insert().values(
                        revision=plan.revision,
                        schema_hash=actual_hash,
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
    # STATE REFRESH (when no operations but hash has drifted)
    # ------------------------------------------------------------------

    def refresh_state_if_needed(self, plan: MigrationPlan) -> None:
        """Re-record migration state when the hash computation logic changed."""
        now = datetime.now(timezone.utc)

        for schema in self.schemas:
            state_cls = _SCHEMA_STATE[schema]

            with self.rbac_db._engine.begin() as conn:
                row = conn.execute(select(state_cls).order_by(state_cls.created_at.desc()).limit(1)).first()
                if row is None:
                    continue
                actual_hash = self._compute_db_hash(conn, schema)
                if row.schema_hash == actual_hash:
                    continue

                log_i(mod, f"Refreshing state for '{schema}' ({row.schema_hash[:12]}… → {actual_hash[:12]}…)")

                conn.execute(
                    state_cls.__table__.insert().values(
                        revision=plan.revision,
                        schema_hash=actual_hash,
                        applied_at=now,
                        applied_by="system",
                        operation_count=0,
                        metadata_snapshot={},
                        details={},
                    )
                )

                log_i(mod, f"Migration state refreshed — revision {plan.revision}")

    # ------------------------------------------------------------------
    # OWNERSHIP + BACKUP requirements
    # ------------------------------------------------------------------

    def ensure_table_ownership(self, dbsu_url) -> None:
        """Transfer ownership of tracked schema objects to the owning user so DDL can run."""
        for schema, owner in self.schema_owners.items():
            # Grant USAGE on schema (lost when pg_restore --clean recreates them)
            subprocess.run(
                ["psql", "-d", dbsu_url, "-c", f"GRANT USAGE ON SCHEMA {schema} TO {owner}"],
                check=True,
                capture_output=True,
                text=True,
            )

            subprocess.run(
                ["psql", "-d", dbsu_url, "-c", f"ALTER SCHEMA {schema} OWNER TO {owner}"],
                check=True,
                capture_output=True,
                text=True,
            )

            # Transfer table/view ownership
            cmd = [
                "psql",
                "-d",
                dbsu_url,
                "-c",
                f"""
                DO $$DECLARE
                  r RECORD;
                BEGIN
                  FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = '{schema}'
                  LOOP
                    EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', '{schema}', r.tablename, '{owner}');
                  END LOOP;
                  FOR r IN SELECT viewname AS tablename FROM pg_views WHERE schemaname = '{schema}'
                  LOOP
                    EXECUTE format('ALTER VIEW %I.%I OWNER TO %I', '{schema}', r.tablename, '{owner}');
                  END LOOP;
                END$$;
                """,
            ]
            subprocess.run(cmd, check=True, capture_output=True, text=True)

    def _table_exists(self, schema: str, table: str, url: str) -> bool:
        """Read-only: does the given table already exist in the DB?"""
        engine = create_engine(url)
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT 1 FROM information_schema.tables " "WHERE table_schema = :schema AND table_name = :name"
                    ),
                    {"schema": schema, "name": table},
                ).first()
                return row is not None
        finally:
            engine.dispose()

    def clean_orphans(self, plan: MigrationPlan) -> None:
        """
        Delete rows that would violate Foreign Key constraints the plan is about to add.

        Only runs for FK relationships where BOTH tables already exist: on a fresh
        DB the referencing tables do not exist yet (the plan is about to create
        them), so there is nothing to clean and the DELETE would otherwise fail.
        """
        conn_str = self.dbsu_url or self.rbac_url
        checks = [
            c
            for c in _build_fk_checks(list(self.schemas), plan)
            if self._table_exists(c.src_schema, c.src_table, conn_str)
        ]
        if not checks:
            return
        log_i(mod, f"Cleaning orphan rows for {len(checks)} FK relationships")
        _delete_orphans(checks, conn_str)

    # ------------------------------------------------------------------
    # REQUIREMENT CHECKS (read-only, run before any mutation)
    # ------------------------------------------------------------------

    def check_analysis_requirements(self) -> None:
        """Verify the owner connection can read the migrated schemas before analysis."""
        try:
            with self.rbac_db._engine.connect() as conn:
                for schema in self.schemas:
                    conn.execute(
                        text("SELECT 1 FROM information_schema.schemata WHERE schema_name = :s"),
                        {"s": schema},
                    ).first()
        except Exception as e:
            raise RuntimeError(
                f"Cannot read schemas for analysis with the configured rbac connection" f" ({self.rbac_user}): {e}"
            ) from e

    def _can_connect(self, url: str, label: str) -> bool:
        """Read-only: can we authenticate and run a trivial query with this connection?"""
        try:
            engine = create_engine(url)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1")).first()
            engine.dispose()
            return True
        except Exception as e:
            log_w(mod, f"Connectivity check failed for {label}: {e}")
            return False

    def check_schema_ownership(self, schema: str, owner: str, url: str) -> bool:
        """Read-only: does the given schema belong to its intended owner?"""
        try:
            engine = create_engine(url)
            with engine.connect() as conn:
                row = conn.execute(
                    text("SELECT nspowner::regrole::text FROM pg_namespace WHERE nspname = :s"),
                    {"s": schema},
                ).first()
            engine.dispose()
            return row is not None and row[0] == owner
        except Exception:
            return False

    def check_schemas_ownership(self) -> bool:
        """Read-only: does each migrated schema belong to its intended owner?"""
        for schema, (owner, url) in self.schema_connections.items():
            if not self.check_schema_ownership(schema, owner, url):
                return False
        return True

    def check_backup_writable(self) -> None:
        backup_prefix = os.environ.get(KRONICLE_RBAC_BACKUP, "./backup/kronicle")
        backup_dir = backup_path(backup_prefix, "rbac").parent
        if not os.access(backup_dir, os.W_OK):
            raise RuntimeError(f"Backup directory is not writable: '{backup_dir}'")

    def check_mutation_requirements(self, plan: MigrationPlan) -> None:
        """
        Validate all requirements of the concrete mutation actions, before the
        user gate and before any mutation. Read-only / non-destructive.

        DDL runs as the owning user when it owns the schemas, so no superuser is
        needed for routine changes. The DB superuser is only required when
        ownership must be (re)established (e.g. after a restore or mismatch).
        """
        if not self.check_schemas_ownership():
            if not self.dbsu_url:
                raise RuntimeError(
                    "The migrated schemas are not owned by their intended owners, so "
                    "ownership transfer is required; dbsu_url is needed but not configured"
                )
            self.ensure_table_ownership(self.dbsu_url)

        self.check_backup_writable()

    def check_tracking_prerequisites(self) -> dict[str, dict]:
        """Check per-schema tracking tables; report the fixes needed (read-only)."""
        fixes: dict[str, dict] = {}
        for schema in self.schemas:
            if not self.check_tracking_tables_exist(schema, self.rbac_url):
                fixes[schema] = {_TRACK_TABLES_CREATION: f"{_TRACK_TABLES_CREATION} '{schema}'"}
        return fixes

    def ensure_tracking_prerequisites(self, fixes: dict[str, dict], *, auto_approve: bool = False) -> None:
        """Create the missing per-schema tracking tables (after operator confirmation)."""
        log_i(mod, "The following tracking-table fixes are needed:")
        for schema, actions in fixes.items():
            for action in actions:
                log_i(mod, f"  - [{schema}] {action}")

        if not auto_approve:
            confirm = input("Apply these tracking-table fixes? (y/n): ")
            if confirm.lower() != "y":
                log_i(mod, "Tracking-table fixes aborted by user")
                raise RuntimeError("Tracking-table fixes not approved")

        with self.rbac_db._engine.begin() as conn:
            self.ensure_tracking_tables(conn)

    # ------------------------------------------------------------------
    # RUN (check → plan → validate → execute)
    # ------------------------------------------------------------------

    def _is_non_destructive(self, plan: MigrationPlan) -> bool:
        return not plan.destructive_ops()

    # ------------------------------------------------------------------
    # BaseProvisioner contract
    # ------------------------------------------------------------------

    def analyze(
        self,
        *,
        auto_approve: bool | None = None,
        verbose: bool = True,
        **kwargs,
    ) -> None:
        """Read-only: resolve the migration plan for core+rbac (and tracking tables)."""
        auto_approve = self.auto_approve if auto_approve is None else auto_approve
        self._auto_approve = auto_approve

        # Phase 1 - analysis requirements (owner can read the schemas)
        self.check_analysis_requirements()

        # Phase 0 - tracking tables (read-only check → confirmed creation when needed).
        # Tracking tables are core+rbac-scope (table-level), not infra.
        tracking_fixes = self.check_tracking_prerequisites()
        if tracking_fixes:
            self.ensure_tracking_prerequisites(tracking_fixes, auto_approve=auto_approve)

        # Pre-check (read stored state, detect drift)
        with self.rbac_db._engine.begin() as conn:
            self.pre_migration_check(conn)

        # Build the plan (no side effects)
        self._plan = self.build_plan()
        self._has_work = bool(self._plan.operations)
        if not self._has_work:
            log_i(mod, "No changes detected — nothing to apply")
            # Re-record state if the hash computation changed (e.g. defaults excluded)
            self.refresh_state_if_needed(self._plan)

    def ask_validation(
        self,
        *,
        auto_approve: bool | None = None,
        auto_approve_if_non_destructive: bool = False,
        verbose: bool = True,
        **kwargs,
    ) -> bool:
        """Present the plan and confirm before any mutation.

        Guard (per user): auto-approve only if non-destructive when using
        ``auto_approve_if_non_destructive``; if the convergence plan unexpectedly
        contains destructive operations, fall back to prompting instead of silently
        dropping things.
        """
        plan = self._plan
        if verbose:
            log_i(mod, f"Plan summary: {plan.summary()}")

        # Phase 2 - execution requirements (schemas owned, backup writable); fail fast.
        self.check_mutation_requirements(plan)

        auto_approve = self._auto_approve
        should_prompt = not auto_approve and not (auto_approve_if_non_destructive and self._is_non_destructive(plan))
        if should_prompt:
            confirm = input("Review above migration plan.\n" "Proceed with backup + migration? (y/n): ")
            if confirm.lower() != "y":
                log_i(mod, "Migration aborted by user")
                return False
        return True

    def backup(self) -> Path:
        backup_prefix = os.environ.get(KRONICLE_RBAC_BACKUP, "./backup/kronicle")
        backup_file = backup_path(backup_prefix, "rbac")
        backup_file.parent.mkdir(parents=True, exist_ok=True)

        cmd = ["pg_dump", "-Fc", "-f", str(backup_file), self._backup_connection_url()]

        for schema in self.schemas:
            cmd += ["-n", schema]

        log_i(mod, f"Creating backup: {backup_file}")
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            log_e(mod, f"Backup failed: {e.stderr}")
            raise RuntimeError(f"Backup failed — aborting migration: {e.stderr}") from e

        return backup_file

    def execute_plan(self, **kwargs) -> None:
        """Apply the validated plan's side effects (clean, apply, record)."""
        plan = self._plan

        # Remove orphan rows in relationship tables that would break FK creation
        self.clean_orphans(plan)

        # Run DDL as the app (owning) user: ownership was ensured during the
        # mutation-requirement check, so no superuser is needed for routine
        # changes. Use a fresh connection, not the tracked session engine.
        with create_engine(self.rbac_url).begin() as conn:
            self.apply_plan(plan, connection=conn)

        self.record_migration_state(plan, success=True)

        if plan.destructive_ops():
            safety = SafetyLevel.DESTRUCTIVE
        elif plan.warning_ops():
            safety = SafetyLevel.WARNING
        else:
            safety = SafetyLevel.SAFE
        self._safety = safety
        self._revision = plan.revision
        self._applied_ops = len(plan.ordered_operations)

    def restore_backup(self, backup_file: Path | str | None) -> None:
        """Roll the schemas back to the saved backup after a failed execution."""
        if not backup_file:
            log_w(mod, "No backup to restore; leaving the database as-is")
            return

        log_i(mod, f"Restoring backup: {backup_file}")
        restore_url = self.dbsu_url or self.rbac_url
        subprocess.run(
            ["pg_restore", "--clean", "--if-exists", "--no-owner", "-d", restore_url, str(backup_file)],
            check=True,
        )

        # Restore privileges lost when schemas/tables are dropped/recreated
        for schema, owner in self.schema_owners.items():
            subprocess.run(
                ["psql", "-d", restore_url, "-c", f"GRANT USAGE ON SCHEMA {schema} TO {owner}"],
                check=True,
                capture_output=True,
                text=True,
            )
            subprocess.run(
                [
                    "psql",
                    "-d",
                    restore_url,
                    "-c",
                    f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema} TO {owner}",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            subprocess.run(
                [
                    "psql",
                    "-d",
                    restore_url,
                    "-c",
                    f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {schema} TO {owner}",
                ],
                check=True,
                capture_output=True,
                text=True,
            )

        # Transfer table ownership so the owning user can run DDL (e.g. ADD CONSTRAINT)
        if self.dbsu_url:
            self.ensure_table_ownership(self.dbsu_url)

        # Re-create the tracking tables as superuser: they were rolled back with the
        # plan transaction and are absent from the restored dump. Hand them back to
        # the owning user so the failure state can actually be recorded.
        with create_engine(restore_url).begin() as conn:
            self.ensure_tracking_tables(conn)
        if self.dbsu_url:
            self.ensure_table_ownership(self.dbsu_url)

        # Record the failure so the state tables reflect it.
        self.record_migration_state(self._plan, success=False)

    def run_post_analysis(self, **kwargs) -> bool:
        """Post-execution verification: the live DB must match metadata.

        Returns True when converged (no leftover operations); False when the plan
        still reports work outstanding (a further ``run_once()`` is needed).
        """
        plan = self.build_plan()
        if plan.operations:
            outstanding = "; ".join(op.describe() for op in plan.ordered_operations)
            log_w(
                mod,
                f"Post-execution analysis: {len(plan.operations)} op(s) outstanding — {outstanding}",
            )
            return False
        log_i(mod, "Post-execution analysis OK — database matches metadata")
        return True


# ======================================================================================
# CLI entrypoint
# ======================================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kronicle RBAC/core schema provisioner")
    parser.add_argument("--secrets", default=None, help="Path to a .secrets file to load")
    parser.add_argument(
        "--auto-approve",
        action="store_true",
        help="Approve prerequisite + migration changes without y/n prompts",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the read-only checks and plan, but do not mutate (do not gate/apply)",
    )
    args = parser.parse_args()

    secrets_env = os.environ.get("KRONICLE_SECRETS_PATH")
    secrets_default = Path(__file__).resolve().parent.parent.parent.parent.parent / ".conf" / ".secrets"
    secrets_path = Path(args.secrets) if args.secrets else Path(secrets_env) if secrets_env else secrets_default

    if secrets_path.exists():
        load_env_file(secrets_path)
        log_d(mod, "Env var loaded")
    else:
        log_d(mod, "Secrets file not found", secrets_path)

    settings = KronicleSettings()
    provisioner = RbacSchemasProvisioner(db_settings=settings.db)

    if args.dry_run:
        provisioner.check_analysis_requirements()
        provisioner.check_tracking_prerequisites()
        plan = provisioner.build_plan()
        if not plan.operations:
            log_i(mod, "No changes detected")
        else:
            log_i(mod, f"Plan summary: {plan.summary()}")
    else:
        provisioner.run_once(auto_approve=args.auto_approve)
