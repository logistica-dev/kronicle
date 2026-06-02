# scripts/init/01_bootstrap_db.py
"""
Idempotent DB bootstrap:

1. Ensure superuser exists (optional)
2. Create application DB
3. Create application user
4. Enable TimescaleDB extension
5. Create SQL schemas
"""

from asyncio import run

from kronicle.db.core.models import CORE_NAMESPACE
from kronicle.db.data.models import DATA_NAMESPACE
from kronicle.db.rbac.models import RBAC_NAMESPACE
from kronicle.utils.str_utils import normalize_pg_identifier
from scripts.utils.logger import log_d, log_w  # type: ignore
from scripts.utils.read_conf import KronicleConf, UserCreds  # type: ignore

mod = "init.01_bootstrap_db"

NAMESPACES = [CORE_NAMESPACE, RBAC_NAMESPACE, DATA_NAMESPACE]


def get_namespace_owners(chan_usr: UserCreds, rbac_usr: UserCreds):
    """
    Map namespace -> owning user
    It is important to have Core namespace before RBAC one,
    so that the tables are created in this order.
    """
    namespace_owners = {
        DATA_NAMESPACE: chan_usr,
        CORE_NAMESPACE: rbac_usr,  # <--- Core first
        RBAC_NAMESPACE: rbac_usr,  # <--- RBAC second
    }
    return namespace_owners


async def ensure_user_exists(db, username: str, password: str) -> str:
    """Ensure a Postgres role exists; create if missing."""
    username = normalize_pg_identifier(username)
    exists = await db.fetchval("SELECT 1 FROM pg_catalog.pg_user WHERE usename=$1", username)
    if not exists:
        await db.execute(
            f"""
            DO $$
            BEGIN
                EXECUTE format('CREATE USER %I WITH PASSWORD %L', '{username}', '{password}');
            END
            $$;
            """
        )
        log_d(mod, f"Created user '{username}'")
    else:
        log_d(mod, f"User '{username}' already exists")
    return username


async def ensure_database_exists(db, db_name: str, owner: str):
    """Ensure a database exists; create if missing."""
    exists = await db.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", db_name)
    db_name = normalize_pg_identifier(db_name)
    owner = normalize_pg_identifier(owner)
    if not exists:
        await db.execute(f"CREATE DATABASE {db_name} OWNER {owner}")
        log_d(mod, f"Created database '{db_name}' owned by '{owner}'")
    else:
        log_d(mod, f"Database '{db_name}' already exists")


async def enable_timescaledb_extension(db):
    """Enable TimescaleDB extension in the target database."""
    await db.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    log_d(mod, "TimescaleDB extension enabled")


async def create_namespaces_if_missing(
    db,
    namespace_owners: dict[str, UserCreds],
    fail_on_owner_mismatch: bool = True,
):
    """
    Ensure schemas exist in the application database with the correct owner.
    Idempotent.

    Args:
        su_conn: asyncpg superuser connection
        namespace_owners: dict mapping namespace -> owning user
        fail_on_owner_mismatch: False if ownership should be enforced/overwriten
    """
    for namespace, owner in namespace_owners.items():
        namespace = normalize_pg_identifier(namespace)
        username = normalize_pg_identifier(owner.username)

        log_d(mod, "Check if schema exists...")
        exists = await db.fetchval("SELECT 1 FROM pg_namespace WHERE nspname=$1", namespace)
        if not exists:
            # Schema does not exist: create it with the intended owner
            await db.execute(f"CREATE SCHEMA {namespace} AUTHORIZATION {username}")
            log_d(mod, f"Created schema '{namespace}' with owner '{username}'")
        else:
            log_d(mod, "Schema exists: check actual owner...")
            current_owner = await db.fetchval(
                "SELECT nspowner::regrole::text FROM pg_namespace WHERE nspname=$1",
                namespace,
            )
            if current_owner != owner.username:
                msg = f"Schema '{namespace}' exists but is owned by '{current_owner}', expected '{username}'"
                if fail_on_owner_mismatch:
                    log_w(mod, msg + " (owner left unchanged, failing)")
                    raise RuntimeError(msg)
                # Optionally, alter the owner instead of failing:
                await db.execute(f"ALTER SCHEMA {namespace} OWNER TO {username}")
                log_w(mod, msg + " (owner changed)")
            else:
                log_d(mod, f"Schema '{namespace}' already exists with correct owner '{username}'")


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

        log_d(mod, f"Ensure application database '{conf.db.name}' exists...")
        await ensure_database_exists(db, conf.db.name, conf.chan_creds.username)

    # --- Connect to application database as superuser to create schemas ---
    namespace_owners = get_namespace_owners(chan_usr=conf.chan_creds, rbac_usr=conf.rbac_creds)

    log_d(mod, "Connecting as superuser to create schemas...")
    async with db_access.session() as su_conn:
        await create_namespaces_if_missing(su_conn, namespace_owners, fail_on_owner_mismatch=False)
    log_d(mod, "Superuser connection closed after schema creation")

    # --- Connect to application database as app user ---
    log_d(mod, f"Connecting to application DB '{db_access.name}' as user '{db_access.usr}'")
    async with db_access.session() as app_conn:
        await enable_timescaledb_extension(app_conn)


if __name__ == "__main__":  # pragma: no cover
    run(main())
