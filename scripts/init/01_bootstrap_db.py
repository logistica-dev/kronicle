# scripts/init/01_bootstrap_db.py
"""
Idempotent DB bootstrap:

1. Ensure superuser exists (optional)
2. Create application DB
3. Create application user
4. Enable TimescaleDB extension
5. Create SQL schemas
"""

import asyncio

from kronicle.db.core.models import CORE_NAMESPACE
from kronicle.db.data.models import DATA_NAMESPACE
from kronicle.db.rbac.models import RBAC_NAMESPACE
from kronicle.deps.settings import validate_pg_identifier
from scripts.utils.logger import log_d  # type: ignore
from scripts.utils.read_conf import KronicleConf  # type: ignore

mod = "init.01_bootstrap_db"

NAMESPACES = [CORE_NAMESPACE, RBAC_NAMESPACE, DATA_NAMESPACE]


async def ensure_user_exists(db, username: str, password: str):
    """Ensure a Postgres role exists; create if missing."""
    validate_pg_identifier(username)
    exists = await db.fetchval("SELECT 1 FROM pg_catalog.pg_user WHERE usename=$1", username)
    if not exists:
        await db.execute(f"CREATE USER {username} WITH PASSWORD $2", password)
        log_d(mod, f"Created user '{username}'")
    else:
        log_d(mod, f"User '{username}' already exists")


async def ensure_database_exists(db, db_name: str, owner: str):
    """Ensure a database exists; create if missing."""
    exists = await db.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", db_name)
    validate_pg_identifier(db_name)
    validate_pg_identifier(owner)
    if not exists:
        await db.execute(f"CREATE DATABASE {db_name} OWNER {owner}")
        log_d(mod, f"Created database '{db_name}' owned by '{owner}'")
    else:
        log_d(mod, f"Database '{db_name}' already exists")


async def enable_timescaledb_extension(db):
    """Enable TimescaleDB extension in the target database."""
    await db.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    log_d(mod, "TimescaleDB extension enabled")


async def main():
    log_d(mod, "Reading configuration...")
    conf: KronicleConf = KronicleConf.read_conf()
    db_access = conf.db

    # --- Connect to "postgres" first as superuser ---
    log_d(mod, "Connecting as superuser to ensure DB and users exist...")
    async with db_access.session(db_name="postgres") as db:
        log_d(mod, "Ensure Channel user exists...")
        await ensure_user_exists(db, conf.chan_creds.username, conf.chan_creds.password)

        log_d(mod, "Ensure RBAC user exists...")
        await ensure_user_exists(db, conf.rbac_creds.username, conf.rbac_creds.password)

        log_d(mod, "Ensure application database exists...")
        await ensure_database_exists(db, conf.db.name, conf.chan_creds.username)

    # --- Connect to application database as app user ---
    log_d(mod, f"Connecting to application DB '{db_access.name}' as user '{db_access.usr}'")
    async with db_access.session() as app_conn:
        await enable_timescaledb_extension(app_conn)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
