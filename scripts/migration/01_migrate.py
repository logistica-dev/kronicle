from __future__ import annotations

import argparse
import os
from pathlib import Path

from kronicle.db.migration.migration_manager import MigrationManager
from kronicle.deps.settings import KronicleSettings
from kronicle.utils.dev_logs import log_d
from kronicle.utils.str_utils import obfuscate_pwd_in_connection_url

if __name__ == "__main__":
    here = "RBAC tables migration"

    parser = argparse.ArgumentParser(description="Run DB migration")
    parser.add_argument(
        "conf_path",
        nargs="?",
        default=None,
        help="Path to .secrets file (optional: falls back to $KRONICLE_SECRETS_PATH or default location)",
    )
    args = parser.parse_args()

    secrets_path = (
        Path(args.conf_path)
        if args.conf_path
        else (
            Path(str(os.environ.get("KRONICLE_SECRETS_PATH")))
            if os.environ.get("KRONICLE_SECRETS_PATH")
            else Path(__file__).resolve().parent.parent.parent / ".conf" / ".secrets"
        )
    )

    if secrets_path.exists():
        import re

        log_d(here, "Secrets found at", secrets_path)

        for line in secrets_path.read_text().splitlines():
            m = re.match(r'^(?:export\s+)?(\w+)\s*=\s*["\']?(.*?)["\']?\s*$', line)
            if m:
                os.environ[m.group(1)] = m.group(2)
        log_d(here, "Env var loaded")
    else:
        log_d(here, "Env file not found", secrets_path)

    settings = KronicleSettings()
    db_url = settings.db.rbac_connection_url
    log_d(here, "db_url", obfuscate_pwd_in_connection_url(db_url))

    # Optional: override backup connection (e.g. superuser) via env var
    backup_url = os.environ.get("KRONICLE_BACKUP_URL") or None
    if backup_url:
        log_d(here, "backup_url", obfuscate_pwd_in_connection_url(backup_url))

    migration_manager = MigrationManager(db_url=db_url, backup_url=backup_url)
    migration_manager.run()
