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
from kronicle.deps.settings import Settings
from kronicle.utils.dev_logs import log_d, log_e, log_w
from kronicle.utils.str_utils import validate_pg_identifier
from scripts.utils.script_utils import get_conf, get_su_connection  # type:ignore

mod = "init.02_create_tables"


def get_namespace_owners(conf: Settings):
    """
    Map namespace -> owning user
    It is important to have Core namespace before RBAC one,
    so that the tables are created in this order.
    """
    namespace_owners = {
        DATA_NAMESPACE: conf.db.usr,
        CORE_NAMESPACE: conf.rbac.usr,  # <--- Core first
        RBAC_NAMESPACE: conf.rbac.usr,  # <--- RBAC second
    }
    return namespace_owners


async def create_namespaces_if_missing(su_conn, namespace_owners: dict[str, str], fail_on_owner_mismatch: bool = True):
    """
    Ensure schemas exist in the application database with the correct owner.

    Args:
        su_conn: asyncpg superuser connection
        namespace_owners: dict mapping namespace -> owning user
        fail_on_owner_mismatch: False if ownership should be enforced/overwriten
    """
    here = "create_namespaces_if_missing"
    for namespace, owner in namespace_owners.items():
        validate_pg_identifier(namespace)
        validate_pg_identifier(owner)

        log_d(here, "Check if schema exists...")
        exists = await su_conn.fetchval("SELECT 1 FROM pg_namespace WHERE nspname=$1", namespace)

        if not exists:
            # Schema does not exist: create it with the intended owner
            await su_conn.execute(f'CREATE SCHEMA "{namespace}" AUTHORIZATION "{owner}"')
            log_d(mod, f"Created schema '{namespace}' with owner '{owner}'")
        else:
            log_d(here, "Schema exists: check actual owner...")
            current_owner = await su_conn.fetchval(
                "SELECT nspowner::regrole::text FROM pg_namespace WHERE nspname=$1",
                namespace,
            )
            if current_owner != owner:
                msg = f"Schema '{namespace}' exists but is owned by '{current_owner}', expected '{owner}'"
                if fail_on_owner_mismatch:
                    log_w(mod, msg + " (owner left unchanged, failing)")
                    raise RuntimeError(msg)
                else:
                    # Optionally, alter the owner instead of failing:
                    await su_conn.execute(f'ALTER SCHEMA "{namespace}" OWNER TO "{owner}"')
                    log_w(mod, msg + " (owner changed)")
            else:
                log_d(mod, f"Schema '{namespace}' already exists with correct owner '{owner}'")


def table_exists(conn, namespace, table_name):
    return conn.execute(
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


def create_pydantic_tables(conn, models, namespace):
    """Create tables from Pydantic models (like ChannelMetadata)"""
    here = f"{mod}.create_pydantic_tables"
    validate_pg_identifier(namespace)
    conn.execute(text(f'SET search_path TO "{namespace}", public'))
    for model in models:
        table_name = model.table_name()
        if table_exists(conn, namespace, table_name):
            log_d(here, f"Table '{namespace}.{table_name}' already exists")
            continue
        conn.execute(text(model.create_table_sql()))
        log_d(here, f"Created table '{namespace}.{table_name}'")


def create_sqlalchemy_tables(conn, tables, namespace):
    """
    Create SQLAlchemy tables or views for a given namespace.

    - Supports KronicleView classes (virtual tables / views)
    - Supports KronicleHierarchyMixin tables
    - Idempotent: checks if table/view exists first
    """
    here = f"{mod}.create_sqlalchemy_tables"
    validate_pg_identifier(namespace)

    # Ensure search_path is set for this connection
    conn.execute(text(f'SET search_path TO "{namespace}", public'))

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
        if table_exists(conn, namespace, table_name):
            log_d(here, "View" if is_view else "Table", f"'{namespace}.{table_name}' already exists")
        else:

            if is_view:
                # Create view if KronicleView
                log_d(here, f"Creating view '{namespace}.{table_name}' ({cls_name})...")
                conn.execute(text(table_cls.create_view_sql()))
                log_d(here, f"Created view '{namespace}.{table_name}'")
                continue  # No hierarchy setup needed, we can skip the rest
            else:
                log_d(here, f"Creating table '{namespace}.{table_name}' ({cls_name})...")

                # Note: ensure schema is set for belt-and-suspenders
                table_obj.schema = namespace
                # Create the table (DDL) and commit immediately
                try:
                    table_obj.create(bind=conn, checkfirst=True)
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
                if table_exists(conn, namespace, hierarchy_table.name):
                    log_d(here, f"Hierarchy table '{namespace}.{hierarchy_table.name}' already exists")
                else:
                    log_d(here, f"Creating hierarchy table '{namespace}.{hierarchy_table.name}'")
                    hierarchy_table.schema = namespace
                    create_sqlalchemy_tables(conn, [hierarchy_table], namespace)


async def main():
    log_d(mod, "Retrieve conf...")
    conf = get_conf()

    log_d(mod, "Ensure superuser exists...")
    su_conn = await get_su_connection(conf.db)

    log_d("----- Step 1: ensure schemas exist with correct owners")
    log_d(mod, "Create namespaces if missing...")
    namespace_owners = get_namespace_owners(conf)
    await create_namespaces_if_missing(su_conn, namespace_owners, fail_on_owner_mismatch=False)
    await su_conn.close()
    log_d(mod, "Superuser connection closed after schema verification")

    log_d("----- Step 2: create tables using the owning users")
    # --- Create tables per owner ---
    for namespace, owner in namespace_owners.items():
        db_url = conf.db.connection_url if owner == conf.db.usr else conf.rbac.connection_url
        log_d(mod, f"Connecting as '{owner}' to create tables in '{namespace}'...")
        engine = create_engine(
            db_url,
            connect_args={"options": f"-c search_path={namespace},public"},
            isolation_level="AUTOCOMMIT",
            future=True,
        )

        with engine.connect() as conn:
            # Set search_path for this connection
            conn.execute(text(f"SET search_path TO {namespace}, public"))

            # Data namespace: Pydantic
            if namespace == DATA_NAMESPACE:
                create_pydantic_tables(conn, ALL_DATA_TABLES, namespace)
            # Core/RBAC: SQLAlchemy
            else:
                tables = ALL_CORE_TABLES if namespace == CORE_NAMESPACE else ALL_RBAC_TABLES
                create_sqlalchemy_tables(conn, tables, namespace)

    log_d(mod, "Tables initialization complete.")


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
