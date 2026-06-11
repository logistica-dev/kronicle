from __future__ import annotations

import argparse
import os
from pathlib import Path

from kronicle.db.migration.migration_manager import MigrationManager
from kronicle.deps.settings import KronicleSettings
from kronicle.utils.dev_logs import log_d
from kronicle.utils.file_utils import load_env_file
from kronicle.utils.str_utils import obfuscate_pwd_in_connection_url


def load_secrets(conf_path: str | None = None) -> None:
    """Load env vars from a .secrets file.

    Precedence:
        1. conf_path (explicit path from caller)
        2. $KRONICLE_SECRETS_PATH env var
        3. Default location: project_root/.conf/.secrets
    """
    secrets_env = os.environ.get("KRONICLE_SECRETS_PATH")
    secrets_path = (
        Path(conf_path)
        if conf_path
        else Path(secrets_env) if secrets_env else Path(__file__).resolve().parent.parent.parent / ".conf" / ".secrets"
    )

    if secrets_path.exists():
        load_env_file(secrets_path)
        log_d("load_secrets", "Env vars loaded from", secrets_path)
    else:
        log_d("load_secrets", "Secrets file not found", secrets_path)


def run_migration(conf_path: str | None = None) -> None:
    """Run the DB migration."""
    here = "run_migration"
    log_d(here, "Loading conf for migration")

    load_secrets(conf_path)

    settings = KronicleSettings()
    db_url = settings.db.rbac_connection_url
    log_d(here, "db_url", obfuscate_pwd_in_connection_url(db_url))

    backup_url = os.environ.get("KRONICLE_BACKUP_URL") or None
    if backup_url:
        log_d(here, "backup_url", obfuscate_pwd_in_connection_url(backup_url))

    log_d(here, "Launching migration")
    migration_manager = MigrationManager(db_url=db_url, backup_url=backup_url)
    migration_manager.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run DB migration")
    parser.add_argument(
        "conf_path",
        nargs="?",
        default=None,
        help="Path to .secrets file (optional: falls back to $KRONICLE_SECRETS_PATH or default location)",
    )
    args = parser.parse_args()

    if not os.environ.get("ALLOW_MIGRATION"):
        print("ALLOW_MIGRATION not set — skipping DB migration")
        raise SystemExit(0)

    run_migration(conf_path=args.conf_path)
