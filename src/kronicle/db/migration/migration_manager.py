# kronicle/db/migration/migration_manager.py
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
from kronicle.db.data.models._registry import DATA_NAMESPACE
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
from kronicle.db.rbac.models._registry import RBAC_NAMESPACE
from kronicle.db.rbac.rbac_db_session import RbacDbSession
from kronicle.db.registry import get_migration_schemas
from kronicle.deps.settings import KronicleSettings
from kronicle.deps.settings_env import KRONICLE_SQLA_BACKUP, DBSettings
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


# ======================================================================================
# MigrationManager
# ======================================================================================
_USER_CREATION = "creation of user"
_SCHEMA_OWNERSHIP = "ownership of schema"
_TRACK_TABLES_CREATION = "creation of the tracking tables for schema"


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
        db_settings: DBSettings,
        alembic_cfg_path: str = "alembic.ini",
        *,
        auto_approve: bool = False,
        backup_url: str | None = None,
    ):
        self.cfg = Config(alembic_cfg_path)
        self._db_settings = db_settings
        self.backup_url = backup_url or self.rbac_url

        self.schemas = get_migration_schemas()
        self._previous_revisions: dict[str, str | None] = {}

        self.rbac_db = RbacDbSession(self.rbac_url)

        self.auto_approve = auto_approve

        # Ownership intent: `core` + `rbac` are owned by the rbac user, `data` by the
        # chan user. Owners/usernames MUST come from DBSettings (which reads them from
        # the configured creds) — never parse them out of a connection URL.
        self._schema_owners = {
            CORE_NAMESPACE: self.rbac_user,
            RBAC_NAMESPACE: self.rbac_user,
            DATA_NAMESPACE: self.chan_user,
        }

        # Per-schema (owner, connection-url) pairs: the app connection for each schema
        # is the schema owner's own connection (rbac URL for core/rbac, chan URL for data).
        self._schema_connections = {
            CORE_NAMESPACE: (self.rbac_user, self.rbac_url),
            RBAC_NAMESPACE: (self.rbac_user, self.rbac_url),
            DATA_NAMESPACE: (self.chan_user, self.data_url),
        }

        # Owner passwords, used only to (re)create a missing role via the dbsu superuser.
        self._owner_passwords = {
            self.rbac_user: self._db_settings._rbac_pwd.get_secret_value(),
            self.chan_user: self._db_settings._chan_pwd.get_secret_value(),
        }

        log_i(mod, f"Migration scope: {self.schemas}")

    @property
    def rbac_user(self) -> str:
        return self._db_settings._rbac_usr

    @property
    def chan_user(self) -> str:
        return self._db_settings._chan_usr

    @property
    def rbac_url(self) -> str:
        return self._db_settings.rbac_connection_url

    @property
    def data_url(self) -> str:
        return self._db_settings.channel_connection_url

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
    # CORE PIPELINE
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
    # STATE REFRESH (when no operat
    #
    # ions but hash has drifted)
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
    # RUN
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

    def clean_orphans(self, plan: MigrationPlan) -> None:
        """Delete rows that would violate FK constraints the plan is about to add."""
        checks = _build_fk_checks(list(self.schemas), plan)
        if not checks:
            return
        conn_str = self.dbsu_url or self.rbac_url
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

    def check_psql_user_exists(self, user: str, url: str) -> bool:
        """Read-only: does the owner role exist in the DB?"""
        if self.dbsu_url:
            log_d(mod, f"Checking if PSQL user '{user}' exists")
            result = subprocess.run(
                ["psql", "-d", self.dbsu_url, "-t", "-A", "-c", f"SELECT 1 FROM pg_roles WHERE rolname = '{user}'"],
                check=True,
                capture_output=True,
                text=True,
            )
            exists = result.stdout.strip() == "1"
        else:
            # No dbsu configured: fall back to attempting a connection with the role.
            log_d(mod, f"Checking connection with PSQL user '{user}'")
            exists = self._can_connect(url, f"owner '{user}'")
        log_d(mod, f"PSQL user '{user}' {'exists' if exists else 'does not exist'}")
        return exists

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

    def ensure_users_exist(self, dbsu_url: str) -> None:
        """Create any missing owner roles via the dbsu superuser connection."""
        for schema, (owner, _) in self.schema_connections.items():
            password = self._owner_passwords.get(owner)
            if password is None:
                raise RuntimeError(f"No password configured for PSQL user '{owner}' — cannot create role")
            log_i(mod, f"Creating PSQL user '{owner}' (owner of schema '{schema}')")
            subprocess.run(
                ["psql", "-d", dbsu_url, "-c", f"CREATE ROLE \"{owner}\" LOGIN PASSWORD '{password}'"],
                check=True,
                capture_output=True,
                text=True,
            )

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

    def check_backup_writable(self) -> None:
        backup_prefix = os.environ.get(KRONICLE_SQLA_BACKUP, "./backup/kronicle")
        backup_dir = Path(backup_prefix).parent
        if not os.access(backup_dir, os.W_OK):
            raise RuntimeError(f"Backup directory is not writable: {backup_dir}")

    def check_prerequisites(self) -> dict[str, dict]:
        """
        Check DB prerequisites per schema (owner role, schema ownership,
        tracking tables), gather the operations needed, confirm with the
        operator, then apply them.

        Always runs at startup; the same path is used by the migration CLI, so the
        CLI is exactly what startup will run (no special modes).
        """
        fixes: dict[str, dict] = {}

        # --- gather (read-only) ---
        # Report every fix needed per schema, not just the first blocker: if the owner
        # role is missing we still list the ownership + tracking-table work that will
        # follow once it exists. Ownership failing implies the tracking tables are not
        # yet provisioned for the owning user either. All are applied together below.
        for schema, (psql_user, url) in self.schema_connections.items():
            if not self.check_psql_user_exists(psql_user, url):
                fixes[schema] = {
                    _USER_CREATION: f"{_USER_CREATION} '{psql_user}' for schema '{schema}'",
                    _SCHEMA_OWNERSHIP: f"{_SCHEMA_OWNERSHIP} '{schema}' by user '{psql_user}'",
                    _TRACK_TABLES_CREATION: f"{_TRACK_TABLES_CREATION} '{schema}'",
                }
            elif not self.check_schema_ownership(schema, psql_user, url):
                fixes[schema] = {
                    _SCHEMA_OWNERSHIP: f"{_SCHEMA_OWNERSHIP} '{schema}' by user '{psql_user}'",
                    _TRACK_TABLES_CREATION: f"{_TRACK_TABLES_CREATION} '{schema}'",
                }
            elif not self.check_tracking_tables_exist(schema, url):
                fixes[schema] = {_TRACK_TABLES_CREATION: f"{_TRACK_TABLES_CREATION} '{schema}'"}
        return fixes

    def ensure_prerequisites(self, fixes: dict[str, dict], *, auto_approve: bool = False):
        """
        Rule: every validation/migration/ownership change in the DB must be explicitly
        confirmed by the operator with a (y/n) prompt before any mutation — never applied
        silently unless auto approve flag is True.
        Same rule as the migration-plan gate in run().
        """
        log_i(mod, "The following fixes are needed:")
        for schema, actions in fixes.items():
            log_i(mod, f"- schema '{schema}'")
            for action in actions:
                log_i(mod, f"    + {action}")

        if not auto_approve:
            confirm = input("Apply these prerequisite fixes? (y/n): ")
            if confirm.lower() != "y":
                log_i(mod, "Prerequisite fixes aborted by user")
                raise RuntimeError("Prerequisite fixes not approved")

        # --- apply (mutation, in the right order) ---
        # Order matters: create missing roles first, then (re)establish schema/table
        # ownership so the owning user can act, then create the tracking tables.
        user_creation_needed = any(_USER_CREATION in fix for fix in fixes.keys())
        schema_ownership_needed = any(_SCHEMA_OWNERSHIP in fix for fix in fixes.keys())
        needs_dbsu = user_creation_needed or schema_ownership_needed
        if needs_dbsu:
            if not self.dbsu_url:
                if user_creation_needed:
                    log_e(mod, f"DB superuser connection needed for {_USER_CREATION} and {_SCHEMA_OWNERSHIP}")
                else:
                    log_e(mod, f"DB superuser connection needed for {_SCHEMA_OWNERSHIP}")
                raise RuntimeError("Prerequisite fixes require dbsu_url (DB superuser) but it is not configured")
            if user_creation_needed:
                self.ensure_users_exist(self.dbsu_url)
            self.ensure_table_ownership(self.dbsu_url)

        with self.rbac_db._engine.begin() as conn:
            self.ensure_tracking_tables(conn)

    def run(self, *, auto_approve: bool | None = None, verbose: bool = True):  # noqa: C901
        """Run the full migration pipeline (prerequisites → analysis → plan → gate → apply).

        This is the exact path the application runs at startup (no special modes).
        """
        log_i(mod, "Starting migration pipeline")

        # Per-call override for the confirmation gates; falls back to the instance
        # auto_approve (from __init__/CLI).
        auto_approve = self.auto_approve if auto_approve is None else auto_approve

        plan = None
        backup_file = None

        try:

            # --------------------------------------------------
            # PHASE 0 - PREREQUISITES
            # --------------------------------------------------
            fixes = self.check_prerequisites()

            if not fixes:
                log_i(mod, "All prerequisites satisfied")
            else:
                self.ensure_prerequisites(fixes, auto_approve=auto_approve)

            # --------------------------------------------------
            # PHASE 1 - ANALYSIS REQUIREMENTS (read-only)
            # --------------------------------------------------
            self.check_analysis_requirements()

            # --------------------------------------------------
            # PRE-CHECK (read stored state, detect drift)
            # --------------------------------------------------
            with self.rbac_db._engine.begin() as conn:
                self.pre_migration_check(conn)

            # --------------------------------------------------
            # BUILD PLAN (no side effects)
            # --------------------------------------------------
            plan = self.build_plan()

            if not plan.operations:
                log_i(mod, "No changes detected — nothing to apply")
                # Re-record state if the hash computation changed (e.g. defaults excluded)
                self.refresh_state_if_needed(plan)
                return

            if verbose:
                log_i(mod, f"Plan summary: {plan.summary()}")

            # --------------------------------------------------
            # PHASE 2 - EXECUTION REQUIREMENTS (now we know the actions)
            #    Fail fast before the gate / any mutation.
            # --------------------------------------------------
            self.check_mutation_requirements(plan)

            # --------------------------------------------------
            # 3. USER GATE (before ANY mutation)
            # --------------------------------------------------
            if not auto_approve:
                confirm = input("Review above migration plan.\n" "Proceed with backup + migration? (y/n): ")
                if confirm.lower() != "y":
                    log_i(mod, "Migration aborted by user")
                    return

            # --------------------------------------------------
            # 4. SIDE EFFECTS (only after approval)
            # --------------------------------------------------
            backup_file = self.backup()

            # Remove orphan rows that would break FK creation
            self.clean_orphans(plan)

            # Run DDL as the app (owning) user: ownership was ensured during the
            # mutation-requirement check, so no superuser is needed for routine
            # changes. Use a fresh connection, not the tracked session engine.
            with create_engine(self.rbac_url).begin() as conn:
                self.apply_plan(plan, connection=conn)

            self.record_migration_state(plan, success=True)

        except Exception as e:
            log_i(mod, f"FAILED migration: {e}")

            if backup_file:
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

                # Re-create the tracking tables as superuser: they were
                # rolled back with the plan transaction and are absent from
                # the restored dump. Hand them back to the owning user so the
                # failure state can actually be recorded.
                with create_engine(restore_url).begin() as conn:
                    self.ensure_tracking_tables(conn)
                if self.dbsu_url:
                    self.ensure_table_ownership(self.dbsu_url)

            if plan is not None:
                self.record_migration_state(plan, success=False)

            raise

        log_i(mod, "Migration completed successfully")


# ======================================================================================
# CLI entrypoint
# ======================================================================================


if __name__ == "__main__":
    here = "migr_manager"

    parser = argparse.ArgumentParser(description="Kronicle DB migration manager")
    parser.add_argument(
        "--secrets",
        default=None,
        help="Path to a .secrets file to load (default: $KRONICLE_SECRETS_PATH or .conf/.secrets)",
    )
    args = parser.parse_args()

    secrets_env = os.environ.get("KRONICLE_SECRETS_PATH")
    secrets_default = Path(__file__).resolve().parent.parent.parent.parent.parent / ".conf" / ".secrets"
    secrets_path = Path(args.secrets) if args.secrets else Path(secrets_env) if secrets_env else secrets_default
    if secrets_path.exists():
        load_env_file(secrets_path)
        log_d(here, "Env var loaded")
    else:
        log_d(here, "Secrets file not found", secrets_path)

    settings = KronicleSettings()
    log_d(here, "db_url", settings.db.masked_rbac_connection_url)
    log_d(here, "data_url", settings.db.masked_connection_url)
    log_d(here, "dbsu_url", settings.db.masked_dbsu_connection_url)

    # Optional: override backup connection (e.g. superuser) via env var
    backup_url = os.environ.get("KRONICLE_BACKUP_URL") or None

    manager = MigrationManager(
        db_settings=settings.db,
        backup_url=backup_url,
    )
    manager.run()
