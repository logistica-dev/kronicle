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

from asyncpg import connect

from kronicle.db.core.models import CORE_NAMESPACE
from kronicle.db.data.models import DATA_NAMESPACE
from kronicle.db.rbac.models import RBAC_NAMESPACE
from kronicle.deps.settings import validate_pg_identifier
from kronicle.utils.dev_logs import log_d
from scripts.utils.script_utils import get_conf, get_su_connection  # type:ignore

mod = "init.01_bootstrap_db"

NAMESPACES = [CORE_NAMESPACE, RBAC_NAMESPACE, DATA_NAMESPACE]


async def ensure_user_exists(su_conn, username: str, password: str):
    """Ensure a Postgres role exists; create if missing."""
    validate_pg_identifier(username)
    exists = await su_conn.fetchval("SELECT 1 FROM pg_catalog.pg_user WHERE usename=$1", username)
    if not exists:
        await su_conn.execute(f"CREATE USER {username} WITH PASSWORD $2", password)
        log_d(mod, f"Created user '{username}'")
    else:
        log_d(mod, f"User '{username}' already exists")


async def ensure_database_exists(su_conn, db_name: str, owner: str):
    """Ensure a database exists; create if missing."""
    exists = await su_conn.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", db_name)
    validate_pg_identifier(db_name)
    validate_pg_identifier(owner)
    if not exists:
        await su_conn.execute(f"CREATE DATABASE {db_name} OWNER {owner}")
        log_d(mod, f"Created database '{db_name}' owned by '{owner}'")
    else:
        log_d(mod, f"Database '{db_name}' already exists")


async def enable_timescaledb_extension(app_conn):
    """Enable TimescaleDB extension in the target database."""
    await app_conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    log_d(mod, "TimescaleDB extension enabled")


async def main():
    log_d(mod, "Retrieve conf...")
    conf = get_conf()

    log_d(mod, "Ensure superuser exists...")
    su_conn = await get_su_connection(conf.db, db="postgres")
    try:
        log_d(mod, "Ensure app user exists...")
        await ensure_user_exists(su_conn, conf.db.usr, conf.db.pwd)

        log_d(mod, "Ensure RBAC user exists...")
        await ensure_user_exists(su_conn, conf.rbac.usr, conf.rbac.pwd)

        log_d(mod, "Ensure application database exists...")
        await ensure_database_exists(su_conn, conf.db.name, conf.db.usr)
    finally:
        await su_conn.close()
        log_d(mod, "Superuser connection closed")

    # Connect to the application database as the app user
    log_d(mod, f"Connecting to database '{conf.db.name}' as user '{conf.db.usr}'")
    app_conn = await connect(dsn=conf.db.connection_url)
    try:
        log_d(mod, "Enabling TimescaleDB extension...")
        await enable_timescaledb_extension(app_conn)
    finally:
        await app_conn.close()
        log_d(mod, "Application DB connection closed")


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
