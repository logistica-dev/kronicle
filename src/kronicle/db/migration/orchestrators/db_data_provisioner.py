# kronicle/db/migration/orchestrators/data_provisioner.py
"""
DataSchemaProvisioner: schema-level reconciliation for the `data` schema.

The `data` schema holds two kinds of tables:
  - a static ``ChannelMetadata`` table (created here via its idempotent DDL, no
    longer requiring the external ``02_create_tables`` init script);
  - per-channel ``data.channel_<hex>`` timeseries tables, created on-the-fly by the
    application (now as TimescaleDB hypertables with a composite ``(time, row_id)``
    primary key).

This provisioner ensures the `chan` (owner) user actually has a writable object in
the schema — the migration tracking tables, mirroring core/rbac — creates the
``ChannelMetadata`` table, and reconciles the channel tables that already exist
(created before the hypertable change): verifying the required system columns, the
TimescaleDB-compatible composite primary key, and that each table is a hypertable,
transforming any that are not.

It follows the shared BaseProvisioner contract (analyze -> ask_validation ->
backup -> execute_plan -> run_post_analysis -> restore_backup on failure) and is
fully synchronous, driven through the `psql` client as the `chan` owner — no
asyncpg, no SQLAlchemy. Owner access suffices for all of its operations (schema
ownership for the tracking tables, table ownership for the PK + hypertable DDL).

MigrationManager is intentionally NOT used here; it is kept as a fallback until
it is retired.
"""

from __future__ import annotations

import argparse
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from kronicle.db.data.models._registry import DATA_NAMESPACE, ChannelMetadata
from kronicle.db.migration.engine.operations import SafetyLevel
from kronicle.db.migration.orchestrators.provisioner_base import ApplyResult, BaseProvisioner, backup_path
from kronicle.deps.settings import KronicleSettings
from kronicle.deps.settings_env import KRONICLE_DATA_BACKUP, DBSettings
from kronicle.utils.dev_logs import log_d, log_e, log_i, log_w
from kronicle.utils.file_utils import load_env_file

mod = "data_schema"

# System columns every channel timeseries table must carry, with their canonical types.
SYSTEM_COLUMNS: dict[str, str] = {
    "time": "timestamp with time zone",
    "row_id": "bigint",
    "received_at": "timestamp with time zone",
}
# TimescaleDB requires the partitioning column to be part of the primary key.
TARGET_PK_COLUMNS = ("time", "row_id")

_TRACK_CREATION = "creation of the tracking tables for schema"


@dataclass
class ChannelDrift:
    """Detected gap(s) between a channel table and the desired hypertable shape."""

    table: str
    missing_columns: list[str] = field(default_factory=list)
    pk_columns: list[str] = field(default_factory=list)
    is_hypertable: bool = False


class DataSchemaProvisioner(BaseProvisioner):
    """Reconcile the `data` schema: tracking tables + channel hypertables."""

    def __init__(
        self,
        db_settings: DBSettings,
        *,
        auto_approve: bool = False,
        backup_url: str | None = None,
    ):
        self._db_settings = db_settings
        self.auto_approve = auto_approve
        self.channels: list[ChannelDrift] = []
        self.backup_url = backup_url or self.chan_url

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    @property
    def chan_user(self) -> str:
        return self._db_settings._chan_usr

    @property
    def chan_url(self) -> str:
        return self._db_settings.channel_connection_url

    @property
    def data_url(self) -> str:
        return self.chan_url

    def _backup_connection_url(self) -> str:
        return self.data_url

    def _psql(self, sql: str, *, url: str | None = None, tuples: bool = False) -> str:
        """Run a statement via psql and return its stdout (trailing newline stripped)."""
        cmd = ["psql", "-d", url or self.data_url, "-c", sql]
        if tuples:
            cmd += ["-t", "-A"]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return result.stdout.strip()

    def _require_tables(self) -> bool:
        """True when the data schema is reachable (owner can run trivial queries)."""
        try:
            return self._psql("SELECT 1", tuples=True) == "1"
        except subprocess.CalledProcessError:
            return False

    # ------------------------------------------------------------------
    # Read-only catalog (atomic checks, explicit via psql)
    # ------------------------------------------------------------------

    def list_channel_tables(self) -> list[str]:
        """Read-only: every timeseries table in the data schema (``channel_<32 hex>``).

        Channel table names are ``channel_`` + the channel id as 32 lowercase hex chars
        (``ChannelTimeseries.table()``). A plain ``LIKE 'channel\\_%'`` would also match the
        static ``channel_metadata`` table, so match on the exact 32-hex shape to exclude it.
        """
        out = self._psql(
            "SELECT tablename FROM pg_tables "
            f"WHERE schemaname = '{DATA_NAMESPACE}' "
            "AND tablename ~ '^channel_[0-9a-f]{32}$' "
            "ORDER BY tablename",
            tuples=True,
        )
        return [line for line in out.splitlines() if line]

    def _table_columns(self, table: str) -> dict[str, str]:
        out = self._psql(
            "SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_schema = '{DATA_NAMESPACE}' AND table_name = '{table}'",
            tuples=True,
        )
        cols: dict[str, str] = {}
        for line in out.splitlines():
            if not line:
                continue
            if "\t" not in line:
                continue
            name, typ = line.split("\t", 1)
            cols[name] = typ
        return cols

    def _primary_key_columns(self, table: str) -> list[str]:
        out = self._psql(
            "SELECT a.attname FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indrelid "
            "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey) "
            f"WHERE c.relname = '{table}' AND c.relnamespace = '{DATA_NAMESPACE}'::regnamespace "
            "AND i.indisprimary ORDER BY a.attnum",
            tuples=True,
        )
        return [line for line in out.splitlines() if line]

    def _primary_key_name(self, table: str) -> str | None:
        out = self._psql(
            "SELECT conname FROM pg_constraint "
            f"WHERE conrelid = '{DATA_NAMESPACE}.{table}'::regclass AND contype = 'p'",
            tuples=True,
        )
        return out or None

    def _is_hypertable(self, table: str) -> bool:
        out = self._psql(
            "SELECT 1 FROM timescaledb_information.hypertables "
            f"WHERE hypertable_schema = '{DATA_NAMESPACE}' AND hypertable_name = '{table}'",
            tuples=True,
        )
        return out == "1"

    def check_tracking_tables_exist(self) -> bool:
        """Read-only: do the data migration tracking tables exist?"""
        out = self._psql(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{DATA_NAMESPACE}' "
            "AND table_name IN ('schema_migration_state', 'schema_migration_history')",
            tuples=True,
        )
        present = {line for line in out.splitlines() if line}
        return {"schema_migration_state", "schema_migration_history"} <= present

    def check_channel_metadata_table_exists(self) -> bool:
        """Read-only: does the static ChannelMetadata table exist in the data schema?"""
        out = self._psql(
            "SELECT 1 FROM information_schema.tables "
            f"WHERE table_schema = '{DATA_NAMESPACE}' AND table_name = '{ChannelMetadata.tablename()}'",
            tuples=True,
        )
        return out == "1"

    # ------------------------------------------------------------------
    # BaseProvisioner contract
    # ------------------------------------------------------------------

    def analyze(self, **kwargs) -> None:
        """Read-only: catalogue channel tables, tracking tables, and metadata gaps."""
        self._has_work = False
        self._tracking_missing = not self.check_tracking_tables_exist()
        self._metadata_missing = not self.check_channel_metadata_table_exists()

        self.channels = []
        for table in self.list_channel_tables():
            columns = self._table_columns(table)
            missing = [c for c in SYSTEM_COLUMNS if c not in columns]
            pk = self._primary_key_columns(table)
            is_hype = self._is_hypertable(table)
            drift = ChannelDrift(
                table=table,
                missing_columns=missing,
                pk_columns=pk,
                is_hypertable=is_hype,
            )
            self.channels.append(drift)

            needs_transform = missing or tuple(pk) != TARGET_PK_COLUMNS or not is_hype
            if needs_transform:
                self._has_work = True

        if self._tracking_missing or self._metadata_missing:
            self._has_work = True

        if not self._has_work:
            log_i(mod, "Data schema is already converged (tracking tables + channel hypertables).")

    def ask_validation(
        self,
        *,
        auto_approve: bool | None = None,
        auto_approve_if_non_destructive: bool = False,
        **kwargs,
    ) -> bool:
        """Present the plan and confirm before any mutation."""
        auto_approve = self.auto_approve if auto_approve is None else auto_approve

        if self._tracking_missing:
            log_w(mod, f"  - [{DATA_NAMESPACE}] {_TRACK_CREATION}")
        if self._metadata_missing:
            log_w(mod, f"  - [{DATA_NAMESPACE}] create ChannelMetadata table")

        for drift in self.channels:
            issues = []
            if drift.missing_columns:
                issues.append("missing " + ",".join(drift.missing_columns))
            if tuple(drift.pk_columns) != TARGET_PK_COLUMNS:
                issues.append(f"PK {list(drift.pk_columns)} != {list(TARGET_PK_COLUMNS)}")
            if not drift.is_hypertable:
                issues.append("not a hypertable")
            if issues:
                log_w(mod, f"  - [{DATA_NAMESPACE}] {drift.table}: " + "; ".join(issues))

        should_prompt = not auto_approve and not (auto_approve_if_non_destructive and self._is_non_destructive())
        if should_prompt:
            confirm = input("Review above data-schema plan.\nProceed with backup + reconcile? (y/n): ")
            if confirm.lower() != "y":
                log_i(mod, "Data-schema reconcile aborted by user")
                return False
        return True

    def backup(self) -> Path | str | None:
        """Safeguard pg_dump of the data schema (custom format) as the chan owner."""
        backup_prefix = os.environ.get(KRONICLE_DATA_BACKUP, "./backup/kronicle")
        backup_file = backup_path(backup_prefix, "data")
        backup_file.parent.mkdir(parents=True, exist_ok=True)

        cmd = ["pg_dump", "-Fc", "-f", str(backup_file), self._backup_connection_url()]
        cmd += ["-n", DATA_NAMESPACE]

        log_i(mod, f"Creating safeguard backup: {backup_file}")
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            log_e(mod, f"Backup failed: {e.stderr}")
            raise RuntimeError(f"Backup failed — aborting data reconcile: {e.stderr}") from e
        return backup_file

    def restore_backup(self, backup_file: Path | str | None) -> None:
        """Roll the data schema back to the saved backup after a failed execution."""
        if not backup_file:
            log_w(mod, "No backup to restore; leaving the database as-is")
            return
        log_i(mod, f"Restoring backup: {backup_file}")
        restore_url = self.data_url
        subprocess.run(
            ["pg_restore", "--clean", "--if-exists", "--no-owner", "-d", restore_url, str(backup_file)],
            check=True,
        )

    def execute_plan(self, **kwargs) -> None:
        """Apply the validated changes: tracking, metadata, then channel transforms."""
        applied = 0

        if self._tracking_missing:
            self._ensure_tracking_tables()
            applied += 1

        if self._metadata_missing:
            self._psql(ChannelMetadata.create_table_sql())
            log_i(mod, f"Created table '{ChannelMetadata.table()}'")
            applied += 1

        for drift in self.channels:
            if self._transform_channel(drift):
                applied += 1

        self._safety = SafetyLevel.DESTRUCTIVE if applied else SafetyLevel.SAFE
        self._applied_ops = applied
        log_i(mod, f"Data-schema reconcile applied {applied} change(s)")

    def run_post_analysis(self, **kwargs) -> bool:
        """Re-run the catalogue; return True once everything is in the desired shape."""
        self.analyze()
        return not self._has_work

    # ------------------------------------------------------------------
    # Mutating helpers (idempotent)
    # ------------------------------------------------------------------

    def _ensure_tracking_tables(self) -> None:
        """Create the data migration tracking tables as the chan owner."""
        state_ddl = (
            f"CREATE TABLE IF NOT EXISTS {DATA_NAMESPACE}.schema_migration_state ("
            "revision TEXT NOT NULL, "
            "schema_hash TEXT NOT NULL, "
            "applied_at TIMESTAMPTZ NOT NULL, "
            "applied_by TEXT NOT NULL, "
            "operation_count INTEGER NOT NULL, "
            "metadata_snapshot JSONB NOT NULL"
            ")"
        )
        history_ddl = (
            f"CREATE TABLE IF NOT EXISTS {DATA_NAMESPACE}.schema_migration_history ("
            "revision TEXT NOT NULL, "
            "previous_revision TEXT, "
            "operation_index INTEGER NOT NULL, "
            "operation_type TEXT NOT NULL, "
            "target TEXT NOT NULL, "
            "plan_hash TEXT NOT NULL, "
            "applied_at TIMESTAMPTZ NOT NULL, "
            "applied_by TEXT NOT NULL, "
            "safety_level TEXT NOT NULL, "
            "success BOOLEAN NOT NULL, "
            "rollback_supported BOOLEAN NOT NULL, "
            "operation_payload JSONB NOT NULL"
            ")"
        )
        self._psql(state_ddl)
        self._psql(history_ddl)
        log_i(mod, f"Created tracking tables in {DATA_NAMESPACE}")

    def _transform_channel(self, drift: ChannelDrift) -> bool:
        """Bring one channel table to the hypertable shape. Returns True if changed."""
        if not (drift.missing_columns or tuple(drift.pk_columns) != TARGET_PK_COLUMNS or not drift.is_hypertable):
            return False

        table = drift.table
        full = f"{DATA_NAMESPACE}.{table}"

        if tuple(drift.pk_columns) != TARGET_PK_COLUMNS:
            pk_name = self._primary_key_name(table)
            if pk_name:
                self._psql(f"ALTER TABLE {full} DROP CONSTRAINT {pk_name}")
            # Re-add as composite (time, row_id); time is NOT NULL, unique row_id keeps it valid.
            self._psql(f"ALTER TABLE {full} ADD PRIMARY KEY (time, row_id)")

        if not drift.is_hypertable:
            self._psql(
                f"SELECT * FROM create_hypertable('{full}', 'time', "
                "if_not_exists => TRUE, create_default_indexes => TRUE)"
            )

        log_i(mod, f"Transformed {full} into a hypertable (composite PK, time-leading)")
        return True

    # ------------------------------------------------------------------

    def _is_non_destructive(self) -> bool:
        """PK reshaping + hypertable conversion are structural; treat as destructive."""
        return False


# ======================================================================================
# CLI entrypoint
# ======================================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kronicle data schema provisioner")
    parser.add_argument("--secrets", default=None, help="Path to a .secrets file to load")
    parser.add_argument(
        "--auto-approve",
        action="store_true",
        help="Approve data-schema changes without y/n prompts",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the read-only analysis and report, but do not mutate",
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
    provisioner = DataSchemaProvisioner(db_settings=settings.db)

    if args.dry_run:
        provisioner.analyze()
        if provisioner._tracking_missing:
            log_w(mod, f"[{DATA_NAMESPACE}] {_TRACK_CREATION}")
        if provisioner._metadata_missing:
            log_w(mod, f"[{DATA_NAMESPACE}] create ChannelMetadata table")
        for drift in provisioner.channels:
            log_d(mod, str(drift))
        if not provisioner._has_work:
            log_i(mod, "No changes detected")
    else:
        result: ApplyResult = provisioner.run_once(auto_approve=args.auto_approve)
        if result.failed:
            log_e(mod, result.message)
            raise SystemExit(1)
        if not result.converged:
            raise SystemExit(1)
