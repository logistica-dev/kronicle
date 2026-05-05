# kronicle/db/migration/migration_manager.py
import subprocess
from pathlib import Path

from alembic import command
from alembic.config import Config

from kronicle.db.migration.bootstrap_check import run_bootstrap_checks
from kronicle.db.migration.migration_audit import collect_metadata_snapshot, log_snapshot
from kronicle.db.rbac.rbac_db_session import RbacDbSession
from kronicle.db.registry import *  # noqa
from kronicle.db.registry import get_migration_schemas
from kronicle.deps.settings import KronicleSettings
from kronicle.types.iso_datetime import IsoDateTime
from kronicle.utils.dev_logs import log_i


class MigrationManager:
    def __init__(self, db_url, alembic_cfg_path: str = "alembic.ini"):
        self.cfg = Config(alembic_cfg_path)
        self.db_url = db_url
        self.schemas = get_migration_schemas()
        self.db = RbacDbSession(db_url)

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
    # MIGRATION FLOW
    # ------------------------------------------------------------------
    def run(self, auto_generate: bool = False, verbose: bool = True):
        log_i("migration", "Starting migration pipeline")

        # 1. PRE SNAPSHOT
        pre = collect_metadata_snapshot()
        if verbose:
            log_snapshot(pre, log_i)

        # 2. BOOTSTRAP VALIDATION
        log_i("migration", "Running bootstrap checks")
        with self.db._engine.connect() as db:
            run_bootstrap_checks(db)  # if you extend later, pass connection

        # 3. BACKUP
        backup_file = self.backup()

        # 4. OPTIONAL: generate migration
        if auto_generate:
            log_i("migration", "Generating Alembic revision")
            command.revision(self.cfg, autogenerate=True, message="auto migration")

        # 5. CONFIRMATION GATE
        confirm = input("Apply migration? (y/n): ")
        if confirm.lower() != "y":
            log_i("migration", "Migration aborted by user")
            return

        # 6. APPLY MIGRATION
        try:
            log_i("migration", "Applying Alembic upgrade")
            command.upgrade(self.cfg, "head")
        except Exception as e:
            log_i("migration", f"FAILED migration: {e}")
            log_i("migration", f"Restoring backup: {backup_file}")

            # rollback hook (manual restore step)
            subprocess.run(["pg_restore", "-d", db_url, str(backup_file)], check=True)

            raise

        # 7. POST SNAPSHOT
        post = collect_metadata_snapshot()
        if verbose:
            log_snapshot(post, log_i)

        log_i("migration", "Migration completed successfully")


if __name__ == "__main__":  # pragma: no-cover

    settings = KronicleSettings()
    db_url = settings.db.rbac_connection_url
    manager = MigrationManager(db_url=db_url)
    manager.run()
