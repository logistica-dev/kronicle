# kronicle/db/migration/bootstrap_check.py
from sqlalchemy import inspect

from kronicle.db.base.kronicle_base import Base
from kronicle.db.migration.bootstrap_report import BootstrapReport
from kronicle.db.registry import get_migration_schemas
from kronicle.utils.dev_logs import log_block, log_d, log_e, log_i

mod = "db_bootstrap"


class MigrationProposal:
    def __init__(self, connection):
        self.db = connection
        self.report = BootstrapReport()
        self.schemas = get_migration_schemas()

    def iter_migration_tables(self):
        for table in Base.metadata.tables.values():
            if table.schema in self.schemas:
                yield table

    def validate_metadata_loaded(self):
        """
        Ensures all SQLAlchemy models are imported and registered.
        """
        here = "valid_meta_loaded"

        with log_block(mod, "Checking SQLAlchemy metadata registration"):
            tables = list(self.iter_migration_tables())

            if not tables:
                raise RuntimeError("No tables found in SQLAlchemy metadata. " "Models are not imported correctly.")

            log_i(here, f"Registered tables: {len(tables)}")

            # Show unresolved or suspicious tables
            for table in tables:
                log_d(here, f"{table.fullname}")
                if table.columns is None or len(table.columns) == 0:
                    self.report.add_error(f"Table '{table.schema}.{table.name}' has no columns registered")

    def validate_foreign_keys(self):
        """
        Ensures all FK targets are resolvable.
        """
        here = "valid_fk"
        with log_block(here, "Checking foreign key resolution"):

            unresolved = []

            for table in self.iter_migration_tables():
                log_d(here, f"{table.fullname}")
                for fk in table.foreign_keys:
                    if fk.column is None:
                        unresolved.append(f"{table.schema}.{table.name} -> UNRESOLVED FK ({fk})")

            if unresolved:
                msg = "\n".join(unresolved)
                raise RuntimeError(f"Unresolved foreign keys detected:\n{msg}")

    def check_schema_integrity(self, strict=True):
        """
        Compares SQLAlchemy metadata with actual DB schema (light check).
        """
        here = "valid_schema"

        missing = []
        extra = []

        with log_block(here, "Checking DB schema consistency"):
            try:
                inspector = inspect(self.db)
            except Exception as e:
                log_e(here, e)
                raise

            schema_tables = {schema: set(inspector.get_table_names(schema=schema)) for schema in self.schemas}

            # Missing tables
            for table in self.iter_migration_tables():
                schema = table.schema
                table_name = table.name

                log_d(here, f"{table.fullname}")
                if table_name not in schema_tables[schema]:
                    missing.append(f"Missing table in DB: {schema}.{table_name}")

            # Extra tables
            for schema in self.schemas:
                declared = {t.name for t in self.iter_migration_tables() if t.schema == schema}
                actual = schema_tables[schema]

                for table_name in actual - declared:
                    extra.append(f"{schema}.{table_name}")

        if missing or extra:
            msg = []

            if missing:
                msg.append("Missing tables:\n" + "\n".join(missing))

            if extra:
                msg.append("Extra tables:\n" + "\n".join(extra))
            err_msg = "Schema integrity check failed:\n\n" + "\n\n".join(msg)
            if strict:
                raise RuntimeError(err_msg)

            log_e(here, err_msg)
            return missing, extra

    def run_bootstrap_checks(self):
        """
        Entry point for all migration safety checks.
        """

        self.validate_metadata_loaded()
        self.validate_foreign_keys()
        self.check_schema_integrity()
