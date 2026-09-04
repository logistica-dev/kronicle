# kronicle/db/migration/db_provisioner.py
"""
DbProvisioner: checks and possibly ensures database readiness.

It is the single owner of infrastructure-level prerequisites: the application
database, its owner roles (psql users), the SQL schemas, and the TimescaleDB
extension. It does NOT touch table-level migration concerns (those belong to the
per-schema migration managers).

Design:
  - atomic ``check_*`` methods : read-only, one concern each, taking their inputs
    explicitly (connection URLs / usernames) so they are easy to test and reuse.
  - ``check_readiness()``      : aggregates the atomic checks into a findings dict.
  - ``ensure()``               : idempotent. Applies the read-only-raised fixes, but
    requires dbsu (creating the DB, roles, schemas and enabling an extension all
    need superuser privileges).

The ``check_*`` methods are the ones that existed in MigrationManager, copied and
refined to cover this object's larger scope (DB creation + TimescaleDB extension);
MigrationManager is kept as a fallback/backup until it is retired.
"""

from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path

from sqlalchemy import create_engine, text

from kronicle.db.core.models._registry import CORE_NAMESPACE
from kronicle.db.data.models._registry import DATA_NAMESPACE
from kronicle.db.migration.orchestrators.provisioner_base import BaseProvisioner, backup_path
from kronicle.db.rbac.models._registry import RBAC_NAMESPACE
from kronicle.deps.settings import KronicleSettings
from kronicle.deps.settings_env import KRONICLE_FULL_BACKUP, DBSettings, get_env_var
from kronicle.utils.dev_logs import log_d, log_e, log_i, log_w
from kronicle.utils.file_utils import load_env_file

mod = "db_provisioner"

_TIMESCALEDB_EXT = "timescaledb"


class DbProvisioner(BaseProvisioner):
    """
    Checks and possibly ensures database readiness: DB, owner roles, SQL schemas,
    TimescaleDB extension. See module docstring for the check/ensure split.

    Implements the BaseProvisioner contract (analyze / ask_validation / backup /
    restore_backup / execute_plan / run_post_analysis) driven by the shared
    ``run_once()`` workflow.
    """

    def __init__(self, db_settings: DBSettings, *, auto_approve: bool = False):
        self._db_settings = db_settings
        self.auto_approve = auto_approve

    # ------------------------------------------------------------------
    # Config (resolved once; passed explicitly into the atomic checks)
    # ------------------------------------------------------------------
    @property
    def rbac_url(self) -> str:
        return self._db_settings.rbac_connection_url

    @property
    def dbsu_url(self) -> str | None:
        return self._db_settings.dbsu_connection_url

    @property
    def dbsu_maintenance_url(self) -> str | None:
        return self._db_settings.dbsu_maintenance_url

    @property
    def db_name(self) -> str:
        return self._db_settings.db_name

    # Owner role → schema mapping (a schema is owned by the role of that name).
    @property
    def schema_owners(self) -> dict[str, str]:
        return {
            CORE_NAMESPACE: self._db_settings.rbac_user,
            RBAC_NAMESPACE: self._db_settings.rbac_user,
            DATA_NAMESPACE: self._db_settings.chan_user,
        }

    @property
    def owner_passwords(self) -> dict[str, str]:
        return self._db_settings.owner_passwords

    def _can_connect(self, url: str, label: str) -> bool:
        """Read-only: can we authenticate and run a trivial query with this connection?"""
        try:
            engine = create_engine(url)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1")).first()
            engine.dispose()
            return True
        except Exception as e:
            log_i(mod, f"Connectivity check failed for {label}: {e}")
            return False

    # ------------------------------------------------------------------
    # Atomic read-only checks (one concern each, explicit inputs)
    # ------------------------------------------------------------------
    def check_db_exists(self, dbsu_maintenance_url: str) -> bool:
        """Read-only: does the application database exist?"""
        result = subprocess.run(
            [
                "psql",
                "-d",
                dbsu_maintenance_url,
                "-t",
                "-A",
                "-c",
                f"SELECT 1 FROM pg_database WHERE datname = '{self.db_name}'",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        exists = result.stdout.strip() == "1"
        log_d(mod, f"Database '{self.db_name}' {'exists' if exists else 'does not exist'}")
        return exists

    def check_psql_user_exists(self, user: str, url: str | None) -> bool:
        """Read-only: does the owner role exist at the cluster level (any DB)?"""
        if not url:
            # No dbsu: fall back to attempting a connection with the role.
            log_d(mod, f"Checking connection with PSQL user '{user}'")
            exists = self._can_connect(
                self._db_settings.get_connection_url(user, self.owner_passwords[user]), f"owner '{user}'"
            )
        else:
            log_d(mod, f"Checking if PSQL user '{user}' exists")
            result = subprocess.run(
                ["psql", "-d", url, "-t", "-A", "-c", f"SELECT 1 FROM pg_roles WHERE rolname = '{user}'"],
                check=True,
                capture_output=True,
                text=True,
            )
            exists = result.stdout.strip() == "1"
        log_d(mod, f"PSQL user '{user}' {'exists' if exists else 'does not exist'}")
        return exists

    def check_psql_user_connectable(self, user: str) -> bool:
        """
        Read-only: can the role authenticate with its configured password against the app DB?

        Only meaningful when the app DB exists (callers gate on it): with the DB present,
        a failed connection genuinely indicates wrong/missing credentials or NOLOGIN.
        """
        url = self._db_settings.get_connection_url(user, self.owner_passwords[user])
        ok = self._can_connect(url, f"owner '{user}'")
        log_d(mod, f"PSQL user '{user}' {'can connect' if ok else 'cannot connect (bad/missing password or NOLOGIN)'}")
        return ok

    def check_schema_ownership(self, schema: str, owner: str, url: str) -> bool:
        """Read-only: does the given schema exist and belong to its intended owner?"""
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

    def check_timescaledb_extension(self, url: str) -> bool:
        """Read-only: is the TimescaleDB extension installed in this database?"""
        engine = create_engine(url)
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text("SELECT 1 FROM pg_extension WHERE extname = :e"),
                    {"e": _TIMESCALEDB_EXT},
                ).first()
        finally:
            engine.dispose()
        installed = row is not None
        log_d(mod, f"Extension '{_TIMESCALEDB_EXT}' {'installed' if installed else 'not installed'}")
        return installed

    # ------------------------------------------------------------------
    # Aggregate readiness (builds a findings dict from the atomic checks)
    # ------------------------------------------------------------------
    def check_readiness(self) -> dict[str, list[str]]:  # noqa: C901
        """
        Report what is missing. Uses dbsu (when configured) for precise read-only
        checks; otherwise falls back to attempting the rbac owner connection, which
        is best-effort and may conflate several failure causes (missing DB, missing
        user, auth failure). Never mutates.
        """
        missing_prereq: dict[str, list[str]] = {}
        dbsu_url = self.dbsu_url
        maintenance_url = self.dbsu_maintenance_url

        # --- Application DB
        # (dbsu → precise; else best-effort probe)
        if maintenance_url:
            db_ok = self.check_db_exists(maintenance_url)
            if not db_ok:
                msg = f"application database '{self.db_name}' does not exist"
                log_d(mod, msg)
                missing_prereq.setdefault("db", []).append(msg)
        else:
            db_ok = self._can_connect(self.rbac_url, "application DB (rbac)")
            if not db_ok:
                msg = f"application database '{self.db_name}' is not reachable via the rbac connection"
                log_d(mod, msg)
                missing_prereq.setdefault("db", []).append(msg)

        # --- Owner roles (cluster-level, independent of the DB): existence is checked
        # always via pg_roles; connectability is only meaningful once the app DB exists
        # (a failed connect against a missing DB is ambiguous, not evidence of bad creds).
        for user in sorted(set(self.schema_owners.values())):
            if not self.check_psql_user_exists(user, maintenance_url):
                msg = f"missing owner role '{user}'"
                log_d(mod, msg)
                missing_prereq.setdefault("users", []).append(msg)
            elif db_ok and not self.check_psql_user_connectable(user):
                msg = f"owner role '{user}' cannot connect (wrong/missing password or NOLOGIN)"
                log_d(mod, msg)
                missing_prereq.setdefault("users", []).append(msg)

        # --- Schemas
        # need a live DB to introspect
        if not db_ok:
            return missing_prereq

        probe_url = dbsu_url or self.rbac_url

        for schema, owner in self.schema_owners.items():
            if not self.check_schema_ownership(schema, owner, probe_url):
                msg = f"schema '{schema}' not owned by '{owner}' (missing or wrong owner)"
                log_d(mod, msg)
                missing_prereq.setdefault("schemas", []).append(msg)

        # --- TimescaleDB extension
        if not self.check_timescaledb_extension(probe_url):
            msg = f"extension '{_TIMESCALEDB_EXT}' is not installed"
            log_d(mod, msg)
            missing_prereq.setdefault("extension", []).append(msg)

        return missing_prereq

    # ------------------------------------------------------------------
    # Backup (safeguard before mutating an existing database)
    # ------------------------------------------------------------------
    def backup(self) -> Path | str | None:
        """Safeguard pg_dump of the managed schemas — only when the DB already exists.

        Returns the backup file path, or None when the app DB does not exist yet
        (nothing to preserve before creating it).
        """
        maintenance_url = self.dbsu_maintenance_url
        if not (self.dbsu_url and maintenance_url):
            raise RuntimeError(
                "Database prerequisites (DB, roles, schemas, extension) require dbsu_url "
                "(the DB superuser connection) but it is not configured"
            )
        if not self.check_db_exists(maintenance_url):
            log_d(mod, "Application DB does not exist — skipping safeguard backup")
            return None

        backup_prefix = get_env_var(KRONICLE_FULL_BACKUP, "./backup/kronicle")
        backup_file = backup_path(backup_prefix, "full")
        backup_file.parent.mkdir(parents=True, exist_ok=True)

        cmd = ["pg_dump", "-Fc", "-f", str(backup_file), self.dbsu_url]
        for schema in self.schema_owners:
            cmd += ["-n", schema]

        log_i(mod, f"Creating safeguard backup: {backup_file}")
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            log_e(mod, f"Backup failed: {e.stderr}")
            raise RuntimeError(f"Backup failed — aborting Database provision: {e.stderr}") from e

        return backup_file

    # ------------------------------------------------------------------
    # BaseProvisioner contract
    # ------------------------------------------------------------------

    def analyze(self, *, auto_approve: bool | None = None, **kwargs) -> None:
        """Read-only: report the missing database prerequisites."""
        auto_approve = self.auto_approve if auto_approve is None else auto_approve
        self._auto_approve = auto_approve
        self._missing = self.check_readiness()
        self._has_work = bool(self._missing)
        if not self._has_work:
            log_i(mod, "All database prerequisites satisfied")

    def ask_validation(self, **kwargs) -> bool:
        """Present the missing prerequisites and confirm before mutating."""
        missing = self._missing
        log_w(mod, "The following database prerequisites are missing:")
        for kind, items in missing.items():
            for item in items:
                log_w(mod, f"  - [{kind}] {item}")

        if not self._auto_approve:
            confirm = input("Apply fixes for the missing database prerequisites (requires superuser)? (y/n): ")
            if confirm.lower() != "y":
                log_i(mod, "Database prerequisites fixes aborted by user")
                return False
        return True

    def restore_backup(self, backup_file: Path | str | None) -> None:
        """Infra changes are idempotent and re-runnable; nothing to roll back."""
        if backup_file:
            log_i(mod, f"Infra is idempotent — no restore needed (safeguard backup: {backup_file})")

    def execute_plan(self, **kwargs) -> None:
        """Apply the missing prerequisites (idempotent; requires dbsu)."""
        if not ((dbsu_url := self.dbsu_url) and (maintenance_url := self.dbsu_maintenance_url)):
            raise RuntimeError(
                "Database prerequisites (DB, roles, schemas, extension) require dbsu_url "
                "(the DB superuser connection) but it is not configured"
            )

        self._ensure_users_exist(dbsu_url)

        if not self.check_db_exists(maintenance_url):
            self._ensure_database_exists(maintenance_url)

        self._ensure_schemas(dbsu_url)
        self._ensure_extension(dbsu_url)

        # Number of distinct fixes the analysis detected (approximate "applied ops").
        self._applied_ops = sum(len(items) for items in self._missing.values())
        log_i(mod, "Database prerequisites ensured")

    def run_post_analysis(self, **kwargs) -> bool:
        """Re-check readiness; return True when all prerequisites are satisfied."""
        missing = self.check_readiness()
        if missing:
            for kind, items in missing.items():
                for item in items:
                    log_w(mod, f"  - [{kind}] {item}")
            return False
        log_i(mod, "Post-execution analysis OK — all database prerequisites satisfied")
        return True

    # ------------------------------------------------------------------
    # Individual ensure helpers (idempotent)
    # ------------------------------------------------------------------
    def _ensure_users_exist(self, dbsu_url: str) -> None:
        """Ensure the owner roles exist (cluster-level)."""
        maintenance_url = self.dbsu_maintenance_url
        for user, password in self.owner_passwords.items():
            if self.check_psql_user_exists(user, maintenance_url):
                log_i(mod, f"Role '{user}' already exists")
                continue
            subprocess.run(
                ["psql", "-d", dbsu_url, "-c", f"CREATE ROLE \"{user}\" LOGIN PASSWORD '{password}'"],
                check=True,
                capture_output=True,
                text=True,
            )
            log_i(mod, f"Created role '{user}'")

    def _ensure_database_exists(self, maintenance_url: str) -> None:
        """Ensure the application database exists (must connect to a maintenance DB)."""
        if self.check_db_exists(maintenance_url):
            log_i(mod, f"Database '{self.db_name}' already exists")
            return
        subprocess.run(
            ["psql", "-d", maintenance_url, "-c", f'CREATE DATABASE "{self.db_name}"'],
            check=True,
            capture_output=True,
            text=True,
        )
        log_i(mod, f"Created database '{self.db_name}'")

    def _ensure_schemas(self, dbsu_url: str) -> None:
        """Ensure each SQL schema exists with its intended owner role."""
        for schema, owner in self.schema_owners.items():
            exists, current_owner = self._schema_info(schema, dbsu_url)
            if not exists:
                subprocess.run(
                    ["psql", "-d", dbsu_url, "-c", f'CREATE SCHEMA "{schema}" AUTHORIZATION "{owner}"'],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                log_i(mod, f"Created schema '{schema}' owned by '{owner}'")
            elif current_owner != owner:
                subprocess.run(
                    ["psql", "-d", dbsu_url, "-c", f'ALTER SCHEMA "{schema}" OWNER TO "{owner}"'],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                log_i(mod, f"Reassigned schema '{schema}' to owner '{owner}'")
            else:
                log_i(mod, f"Schema '{schema}' already exists with correct owner")

    def _ensure_extension(self, dbsu_url: str) -> None:
        """Ensure the TimescaleDB extension is installed (database-wide)."""
        if self.check_timescaledb_extension(dbsu_url):
            log_i(mod, f"Extension '{_TIMESCALEDB_EXT}' already installed")
            return
        subprocess.run(
            ["psql", "-d", dbsu_url, "-c", f"CREATE EXTENSION IF NOT EXISTS {_TIMESCALEDB_EXT};"],
            check=True,
            capture_output=True,
            text=True,
        )
        log_i(mod, f"Enabled extension '{_TIMESCALEDB_EXT}'")

    # ------------------------------------------------------------------
    # Read-only introspection helper
    # ------------------------------------------------------------------
    @staticmethod
    def _schema_info(schema: str, url: str) -> tuple[bool, str | None]:
        """Return (exists, current_owner) for a schema via a regular connection."""
        try:
            engine = create_engine(url)
            with engine.connect() as conn:
                row = conn.execute(
                    text("SELECT nspowner::regrole::text FROM pg_namespace WHERE nspname = :s"),
                    {"s": schema},
                ).first()
            engine.dispose()
        except Exception:
            return False, None
        return (row is not None, row[0] if row else None)


if __name__ == "__main__":
    here = "db_provisioner"

    parser = argparse.ArgumentParser(description="Kronicle DB provisioner (infra prerequisites)")
    parser.add_argument("--secrets", default=None, help="Path to a .secrets file to load")
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Run only the read-only readiness check; do not apply fixes",
    )
    parser.add_argument(
        "--auto-approve",
        action="store_true",
        help="Approve prerequisite changes without a y/n prompt",
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
    provisioner = DbProvisioner(db_settings=settings.db, auto_approve=args.auto_approve)

    if args.check_only:
        provisioner.analyze(auto_approve=args.auto_approve)
    else:
        result = provisioner.run_once(auto_approve=args.auto_approve)
        if result.failed:
            log_e(here, result.message)
            raise SystemExit(1)
        if not result.converged:
            raise SystemExit(1)
