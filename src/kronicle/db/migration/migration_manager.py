# kronicle/db/migration/migration_manager.py
from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from json import dumps
from pathlib import Path
from urllib.parse import urlparse

from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect, select, text

from kronicle.db.base.kronicle_base import Base
from kronicle.db.core.models.core_entity import CoreEntity
from kronicle.db.migration.db_catalog import DatabaseCatalogBuilder
from kronicle.db.migration.migration_plan import MigrationPlan
from kronicle.db.migration.migration_proposal import MigrationProposal
from kronicle.db.migration.operations import AddColumnOp
from kronicle.db.migration.persistence.schema_migration_history import (
    CoreSchemaMigrationHistory,
    RbacSchemaMigrationHistory,
)
from kronicle.db.migration.persistence.schema_migration_state import (
    CoreSchemaMigrationState,
    RbacSchemaMigrationState,
)
from kronicle.db.rbac.models.rbac_entity import RbacEntity
from kronicle.db.rbac.rbac_db_session import RbacDbSession
from kronicle.db.registry import get_migration_schemas
from kronicle.deps.settings import KronicleSettings
from kronicle.deps.settings_env import KRONICLE_SQLA_BACKUP
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.utils.dev_logs import log_d, log_e, log_i, log_w
from kronicle.utils.file_utils import load_env_file

mod = "migration"

# --------------------------------------------------------------------------------------
# Schema → model mapping
# --------------------------------------------------------------------------------------
_SCHEMA_STATE: dict[str, type] = {}
_SCHEMA_HISTORY: dict[str, type] = {}


def _register_schema_models():
    if _SCHEMA_STATE:
        return

    _SCHEMA_STATE[CoreEntity.namespace()] = CoreSchemaMigrationState
    _SCHEMA_STATE[RbacEntity.namespace()] = RbacSchemaMigrationState
    _SCHEMA_HISTORY[CoreEntity.namespace()] = CoreSchemaMigrationHistory
    _SCHEMA_HISTORY[RbacEntity.namespace()] = RbacSchemaMigrationHistory


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

    def __init__(
        self,
        db_url: str,
        dbsu_url: str | None = None,
        alembic_cfg_path: str = "alembic.ini",
        *,
        auto_approve: bool = False,
        backup_url: str | None = None,
    ):
        self.cfg = Config(alembic_cfg_path)
        self.db_url = db_url
        self.dbsu_url = dbsu_url
        self.backup_url = backup_url or db_url

        self.schemas = get_migration_schemas()
        self.db = RbacDbSession(db_url)
        self._previous_revisions: dict[str, str | None] = {}

        self.auto_approve = auto_approve

        log_i(mod, f"Migration scope: {self.schemas}")

    # ------------------------------------------------------------------
    # BACKUP
    # ------------------------------------------------------------------

    def _backup_url(self) -> str:
        return self.dbsu_url or self.backup_url

    def backup(self) -> Path:
        backup_prefix = os.environ.get(KRONICLE_SQLA_BACKUP, "./backup/kronicle")
        backup_prefix_path = Path(backup_prefix)

        ts = IsoDateTime.now_utc().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_prefix_path.parent / f"{backup_prefix_path.name}_{ts}.dump"
        backup_file.parent.mkdir(parents=True, exist_ok=True)

        cmd = ["pg_dump", "-Fc", "-f", str(backup_file), self._backup_url()]

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

    def _ensure_tracking_tables(self, conn) -> None:
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

    def _pre_migration_check(self, conn):
        """
        Read current stored state for each schema, compute actual DB hash,
        and detect drift.
        """
        self._ensure_tracking_tables(conn)

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

    def apply_plan(self, plan: MigrationPlan, connection=None) -> None:
        """Execute migration plan via Alembic Operations context."""
        log_i(mod, f"Executing migration plan: {len(plan.ordered_operations)} ops")

        if connection is not None:
            context = MigrationContext.configure(connection)
            ops = Operations(context)
            plan.apply(ops)
        else:
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

        # Tracking tables may be missing on the failure path: they were rolled
        # back with the plan transaction and absent from the restored dump.
        with self.db._engine.begin() as conn:
            self._ensure_tracking_tables(conn)

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

    def _refresh_state_if_needed(self, plan: MigrationPlan) -> None:
        """Re-record migration state when the hash computation logic changed."""
        now = datetime.now(timezone.utc)

        for schema in self.schemas:
            state_cls = _SCHEMA_STATE[schema]

            with self.db._engine.begin() as conn:
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
    # RUN
    # ------------------------------------------------------------------

    def _ensure_table_ownership(self) -> None:
        """Transfer ownership of tracked schema objects to the app user so DDL can run."""
        if not self.dbsu_url:
            return
        app_user = urlparse(self.db_url).username
        if not app_user:
            return
        schema_list = ", ".join(f"'{s}'" for s in self.schemas)

        # Grant USAGE on schemas (lost when pg_restore --clean recreates them)
        for schema in self.schemas:
            subprocess.run(
                ["psql", "-d", self.dbsu_url, "-c", f"GRANT USAGE ON SCHEMA {schema} TO {app_user}"],
                check=True,
                capture_output=True,
                text=True,
            )

        # Transfer table/view ownership
        cmd = [
            "psql",
            "-d",
            self.dbsu_url,
            "-c",
            f"""
            DO $$DECLARE
              r RECORD;
            BEGIN
              FOR r IN SELECT schemaname, tablename FROM pg_tables WHERE schemaname = ANY(ARRAY[{schema_list}])
              LOOP
                EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', r.schemaname, r.tablename, '{app_user}');
              END LOOP;
              FOR r IN SELECT schemaname, viewname AS tablename FROM pg_views WHERE schemaname = ANY(ARRAY[{schema_list}])
              LOOP
                EXECUTE format('ALTER VIEW %I.%I OWNER TO %I', r.schemaname, r.tablename, '{app_user}');
              END LOOP;
            END$$;
            """,
        ]
        subprocess.run(cmd, check=True, capture_output=True, text=True)

    def _clean_orphans(self, plan: MigrationPlan) -> None:
        """Delete rows that would violate FK constraints the plan is about to add."""
        checks = _build_fk_checks(list(self.schemas), plan)
        if not checks:
            return
        conn_str = self.dbsu_url or self.db_url
        log_i(mod, f"Cleaning orphan rows for {len(checks)} FK relationships")
        _delete_orphans(checks, conn_str)

    def run(self, verbose: bool = True):  # noqa: C901
        log_i(mod, "Starting migration pipeline")

        plan = None
        backup_file = None

        try:
            # --------------------------------------------------
            # 0. ENSURE TABLE OWNERSHIP (DDL requires ownership)
            # --------------------------------------------------
            self._ensure_table_ownership()

            # --------------------------------------------------
            # 1. PRE-CHECK (read stored state, detect drift)
            #    A committed transaction: tracking-table DDL must persist so
            #    the plan sees them as "up to date" rather than re-creating
            #    them inside the (rollback-able) plan transaction.
            # --------------------------------------------------
            with self.db._engine.begin() as conn:
                self._pre_migration_check(conn)

            # --------------------------------------------------
            # 2. BUILD PLAN (no side effects)
            # --------------------------------------------------
            plan = self.build_plan()

            if not plan.operations:
                log_i(mod, "No changes detected — nothing to apply")
                # Re-record state if the hash computation changed (e.g. defaults excluded)
                self._refresh_state_if_needed(plan)
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

            # Remove orphan rows that would break FK creation
            self._clean_orphans(plan)

            # Run DDL as dbsu so the app user doesn't need CREATE ON SCHEMA
            if self.dbsu_url:
                dbsu_engine = create_engine(self.dbsu_url)
                with dbsu_engine.begin() as conn:
                    self.apply_plan(plan, connection=conn)
            else:
                self.apply_plan(plan)

            # Transfer ownership back to app user after DDL (postgres created objects)
            self._ensure_table_ownership()

            self.record_migration_state(plan, success=True)

        except Exception as e:
            log_i(mod, f"FAILED migration: {e}")

            if backup_file:
                log_i(mod, f"Restoring backup: {backup_file}")
                restore_url = self.dbsu_url or self.db_url
                subprocess.run(
                    ["pg_restore", "--clean", "--if-exists", "--no-owner", "-d", restore_url, str(backup_file)],
                    check=True,
                )

                # Restore privileges lost when schemas/tables are dropped/recreated
                app_user = urlparse(self.db_url).username
                if app_user:
                    for schema in self.schemas:
                        subprocess.run(
                            ["psql", "-d", restore_url, "-c", f"GRANT USAGE ON SCHEMA {schema} TO {app_user}"],
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
                                f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema} TO {app_user}",
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
                                f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {schema} TO {app_user}",
                            ],
                            check=True,
                            capture_output=True,
                            text=True,
                        )

                    # Transfer table ownership so the app user can run DDL (e.g. ADD CONSTRAINT)
                    self._ensure_table_ownership()

                    # Re-create the tracking tables as superuser: they were
                    # rolled back with the plan transaction and are absent from
                    # the restored dump. Hand them back to the app user so the
                    # failure state can actually be recorded.
                    with create_engine(restore_url).begin() as conn:
                        self._ensure_tracking_tables(conn)
                    self._ensure_table_ownership()

            if plan is not None:
                self.record_migration_state(plan, success=False)

            raise

        log_i(mod, "Migration completed successfully")


# ======================================================================================
# CLI entrypoint
# ======================================================================================


if __name__ == "__main__":
    here = "migr_manager"

    # load .conf/.secrets into os.environ
    # secrets_path = Path(__file__).resolve().parent.parent.parent.parent.parent / ".conf" / ".secrets"
    secrets_path = Path(__file__).resolve().parent.parent.parent.parent.parent / ".conf" / ".rims.secrets"
    if secrets_path.exists():
        load_env_file(secrets_path)
        log_d(here, "Env var loaded")

    settings = KronicleSettings()
    db_url = settings.db.rbac_connection_url
    log_d(here, "db_url", settings.db.masked_rbac_connection_url)
    log_d(here, "dbsu_url", settings.db.dbsu_connection_url)
    dbsu_url = settings.db.dbsu_connection_url
    assert dbsu_url

    # Optional: override backup connection (e.g. superuser) via env var
    backup_url = os.environ.get("KRONICLE_BACKUP_URL") or None

    manager = MigrationManager(db_url=db_url, dbsu_url=dbsu_url, backup_url=backup_url)
    manager.run()
