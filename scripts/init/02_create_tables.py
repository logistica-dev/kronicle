# scripts/init/02_create_tables.py
"""
RBAC + Core + Data tables initialization script.

- Creates schemas as superuser and attirubtes ownership
- Creates SQLAlchemy-based tables as rbac user
- Creates Pydantic-based tables as app user
- Idempotent: safe to run multiple times
"""

import asyncio

from sqlalchemy import create_engine, text

from kronicle.db.base.kronicle_hierarchy import KronicleHierarchyMixin
from kronicle.db.core.models import ALL_CORE_TABLES, CORE_NAMESPACE
from kronicle.db.data.models import ALL_DATA_TABLES, DATA_NAMESPACE
from kronicle.db.rbac.models import ALL_RBAC_TABLES, RBAC_NAMESPACE
from kronicle.utils.str_utils import normalize_pg_identifier
from scripts.utils.logger import log_d, log_e, log_w  # type: ignore
from scripts.utils.read_conf import KronicleConf, UserCreds  # type: ignore

mod = "init.02_create_tables"


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


async def create_namespaces_if_missing(
    db,
    namespace_owners: dict[str, UserCreds],
    fail_on_owner_mismatch: bool = True,
):
    """
    Ensure schemas exist in the application database with the correct owner.

    Args:
        su_conn: asyncpg superuser connection
        namespace_owners: dict mapping namespace -> owning user
        fail_on_owner_mismatch: False if ownership should be enforced/overwriten
    """
    here = "create_namespaces_if_missing"
    for namespace, owner in namespace_owners.items():
        namespace = normalize_pg_identifier(namespace)
        username = normalize_pg_identifier(owner.username)

        log_d(here, "Check if schema exists...")
        exists = await db.fetchval("SELECT 1 FROM pg_namespace WHERE nspname=$1", namespace)

        if not exists:
            # Schema does not exist: create it with the intended owner
            await db.execute(f"CREATE SCHEMA {namespace} AUTHORIZATION {username}")
            log_d(mod, f"Created schema '{namespace}' with owner '{username}'")
        else:
            log_d(here, "Schema exists: check actual owner...")
            current_owner = await db.fetchval(
                "SELECT nspowner::regrole::text FROM pg_namespace WHERE nspname=$1",
                namespace,
            )
            if current_owner != owner.username:
                msg = f"Schema '{namespace}' exists but is owned by '{current_owner}', expected '{username}'"
                if fail_on_owner_mismatch:
                    log_w(mod, msg + " (owner left unchanged, failing)")
                    raise RuntimeError(msg)
                else:
                    # Optionally, alter the owner instead of failing:
                    await db.execute(f"ALTER SCHEMA {namespace} OWNER TO {username}")
                    log_w(mod, msg + " (owner changed)")
            else:
                log_d(mod, f"Schema '{namespace}' already exists with correct owner '{username}'")


def table_exists(db, namespace, table_name):
    return db.execute(
        text(
            """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = :schema AND table_name = :table
                )
                """
        ),
        {"schema": namespace, "table": table_name},
    ).scalar()


def create_pydantic_tables(db, models, namespace):
    """Create tables from Pydantic models (like ChannelMetadata)"""
    here = "create_pydantic_tables"
    namespace = normalize_pg_identifier(namespace)
    db.execute(text(f"SET search_path TO {namespace}, public"))
    for model in models:
        table_name = model.tablename()
        if table_exists(db, namespace, table_name):
            log_d(here, f"Table '{namespace}.{table_name}' already exists")
            continue
        db.execute(text(model.create_table_sql()))
        log_d(here, f"Created table '{namespace}.{table_name}'")


def create_sqlalchemy_tables(db, tables, namespace):
    """
    Create SQLAlchemy tables or views for a given namespace.

    - Supports KronicleView classes (virtual tables / views)
    - Supports KronicleHierarchyMixin tables
    - Idempotent: checks if table/view exists first
    """
    here = "create_sqlalchemy_tables"
    namespace = normalize_pg_identifier(namespace)

    # Ensure search_path is set for this connection
    db.execute(text(f"SET search_path TO {namespace}, public"))

    for table in tables:
        # Declarative models have __table__, standalone Table objects do not
        if hasattr(table, "__table__"):
            table_obj = table.__table__
            table_cls = table
        else:
            table_obj = table
            table_cls = type(table)

        table_name = table.tablename() if hasattr(table, "tablename") else table_obj.name
        cls_name = table_cls.__name__
        is_view = getattr(table_cls, "is_view", False)

        # --- Create table or view ---
        if table_exists(db, namespace, table_name):
            log_d(here, "View" if is_view else "Table", f"'{namespace}.{table_name}' already exists")
        else:

            if is_view:
                # Create view if KronicleView
                log_d(here, f"Creating view '{namespace}.{table_name}' ({cls_name})...")
                db.execute(text(table_cls.create_view_sql()))
                log_d(here, f"Created view '{namespace}.{table_name}'")
                continue  # No hierarchy setup needed, we can skip the rest
            else:
                log_d(here, f"Creating table '{namespace}.{table_name}' ({cls_name})...")

                # Note: ensure schema is set for belt-and-suspenders
                table_obj.schema = namespace
                # Create the table (DDL) and commit immediately
                try:
                    table_obj.create(bind=db, checkfirst=True)
                    log_d(here, f"Created table '{namespace}.{table_name}'")
                except Exception as e:
                    log_e(here, f"Table creation failed for '{namespace}.{table_name}'")
                    raise e

        # --- Setup hierarchy if applicable (KronicleHierarchyMixin) ---
        if hasattr(table_cls, "_setup_hierarchy") and issubclass(table_cls, KronicleHierarchyMixin):
            log_d(here, f"Setting up the hierarchy for '{namespace}.{table_name}'")
            table_cls._setup_hierarchy()
            log_d(here, "Hierarchy set")

            hierarchy_table = getattr(table_cls, "_hierarchy_table", None)
            if hierarchy_table is not None:
                if table_exists(db, namespace, hierarchy_table.name):
                    log_d(here, f"Hierarchy table '{namespace}.{hierarchy_table.name}' already exists")
                else:
                    log_d(here, f"Creating hierarchy table '{namespace}.{hierarchy_table.name}'")
                    hierarchy_table.schema = namespace
                    create_sqlalchemy_tables(db, [hierarchy_table], namespace)


async def main():
    log_d(mod, "Loading configuration...")
    conf = KronicleConf.read_conf()
    namespace_owners = get_namespace_owners(chan_usr=conf.chan_creds, rbac_usr=conf.rbac_creds)

    log_d("----- Step 1: ensure schemas exist with correct owners")
    log_d(mod, "Create namespaces if missing...")
    async with conf.db.session() as su_conn:
        await create_namespaces_if_missing(su_conn, namespace_owners, fail_on_owner_mismatch=False)
    await su_conn.close()
    log_d(mod, "Superuser connection closed after schema verification")

    log_d("----- Step 2: create tables using the owning users")
    # --- Create tables per owner ---
    for namespace, owner in namespace_owners.items():
        dsn = conf.db.dsn(owner)

        log_d(mod, f"Connecting as '{owner.username}' to create tables in '{namespace}'...")
        engine = create_engine(
            dsn,
            connect_args={"options": f"-c search_path={namespace},public"},
            isolation_level="AUTOCOMMIT",
            future=True,
        )

        with engine.connect() as db:
            # Set search_path for this connection
            db.execute(text(f"SET search_path TO {namespace}, public"))

            # Data namespace: Pydantic
            if namespace == DATA_NAMESPACE:
                create_pydantic_tables(db, ALL_DATA_TABLES, namespace)

            # Core/RBAC: SQLAlchemy
            else:
                tables = ALL_CORE_TABLES if namespace == CORE_NAMESPACE else ALL_RBAC_TABLES
                create_sqlalchemy_tables(db, tables, namespace)

    log_d(mod, "Tables initialization complete.")


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
