# kronicle/db/migration/manager.py
import subprocess
from datetime import datetime
from pathlib import Path

from alembic import command
from alembic.config import Config

from kronicle.db.migration.audit import collect_metadata_snapshot, log_snapshot
from kronicle.db.migration.bootstrap_check import run_bootstrap_checks
from kronicle.db.registry import *  # . noqa
from kronicle.utils.dev_logs import log_e


class MigrationManager:
    def __init__(self, alembic_cfg_path: str = "alembic.ini"):
        self.cfg = Config(alembic_cfg_path)

    # ------------------------------------------------------------------
    # BACKUP
    # ------------------------------------------------------------------
    def backup(self, db_url: str) -> Path:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup_file = Path(f"./backups/kronicle_{ts}.dump")
        backup_file.parent.mkdir(exist_ok=True)

        cmd = [
            "pg_dump",
            "-Fc",
            "-f",
            str(backup_file),
            db_url,
        ]

        log_e("migration", f"Creating backup: {backup_file}")
        subprocess.run(cmd, check=True)

        return backup_file

    # ------------------------------------------------------------------
    # MIGRATION FLOW
    # ------------------------------------------------------------------
    def run(self, db_url: str, auto_generate: bool = False, verbose: bool = True):
        log_e("migration", "Starting migration pipeline")

        # 1. PRE SNAPSHOT
        pre = collect_metadata_snapshot()
        if verbose:
            log_snapshot(pre, log_e)

        # 2. BOOTSTRAP VALIDATION
        log_e("migration", "Running bootstrap checks")
        run_bootstrap_checks(None)  # if you extend later, pass connection

        # 3. BACKUP
        backup_file = self.backup(db_url)

        # 4. OPTIONAL: generate migration
        if auto_generate:
            log_e("migration", "Generating Alembic revision")
            command.revision(self.cfg, autogenerate=True, message="auto migration")

        # 5. CONFIRMATION GATE
        confirm = input("Apply migration? (y/n): ")
        if confirm.lower() != "y":
            log_e("migration", "Migration aborted by user")
            return

        # 6. APPLY MIGRATION
        try:
            log_e("migration", "Applying Alembic upgrade")
            command.upgrade(self.cfg, "head")
        except Exception as e:
            log_e("migration", f"FAILED migration: {e}")
            log_e("migration", f"Restoring backup: {backup_file}")

            # rollback hook (manual restore step)
            subprocess.run(["pg_restore", "-d", db_url, str(backup_file)], check=True)

            raise

        # 7. POST SNAPSHOT
        post = collect_metadata_snapshot()
        if verbose:
            log_snapshot(post, log_e)

        log_e("migration", "Migration completed successfully")
