# kronicle/db/migration/bootstrap_check.py
from sqlalchemy import inspect

from kronicle.db.base.kronicle_base import Base
from kronicle.utils.dev_logs import log_block, log_e

mod = "db_bootstrap"


def validate_metadata_loaded():
    """
    Ensures all SQLAlchemy models are imported and registered.
    """

    with log_block(mod, "Checking SQLAlchemy metadata registration"):

        tables = Base.metadata.tables

        if not tables:
            raise RuntimeError("No tables found in SQLAlchemy metadata. " "Models are not imported correctly.")

        log_e(mod, f"Registered tables: {len(tables)}")

        # Show unresolved or suspicious tables
        for name, table in tables.items():
            if table.columns is None or len(table.columns) == 0:
                raise RuntimeError(f"Table '{name}' has no columns registered")


def validate_foreign_keys():
    """
    Ensures all FK targets are resolvable.
    """

    with log_block(mod, "Checking foreign key resolution"):

        unresolved = []

        for table in Base.metadata.tables.values():
            for fk in table.foreign_keys:
                if fk.column is None:
                    unresolved.append(f"{table.fullname} -> UNRESOLVED FK ({fk})")

        if unresolved:
            msg = "\n".join(unresolved)
            raise RuntimeError(f"Unresolved foreign keys detected:\n{msg}")


def validate_schema_integrity(connection):
    """
    Compares SQLAlchemy metadata with actual DB schema (light check).
    """

    with log_block(mod, "Checking DB schema consistency"):

        inspector = inspect(connection)

        for table_name, table in Base.metadata.tables.items():
            schema = table.schema

            if table_name not in inspector.get_table_names(schema=schema):
                raise RuntimeError(f"Missing table in DB: {schema}.{table_name}")


def run_bootstrap_checks(connection):
    """
    Entry point for all migration safety checks.
    """

    validate_metadata_loaded()
    validate_foreign_keys()
    validate_schema_integrity(connection)
