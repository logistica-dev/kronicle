# kronicle/db/migration/schema_diff_engine.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Set

from sqlalchemy import inspect
from sqlalchemy.engine import Connection
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.schema import MetaData, Table

from kronicle.db.migration.operations import (
    AddColumnOp,
    AlterColumnNullabilityOp,
    AlterColumnTypeOp,
    CreateSchemaOp,
    CreateTableOp,
    DbStructureOperation,
    DropColumnOp,
    DropTableOp,
)
from kronicle.utils.dev_logs import log_d

mod = "schema_diff"


# ==================================================================================================
# Diff result
# ==================================================================================================


@dataclass
class SchemaDiff:
    operations: List[DbStructureOperation] = field(default_factory=list)

    def add(self, op: DbStructureOperation):
        self.operations.append(op)


# ==================================================================================================
# Diff engine
# ==================================================================================================


class SchemaDiffEngine:
    """
    Compare SQLAlchemy metadata against the live database schema
    and generate SchemaOperations.

    Scope:
    - schemas
    - tables
    - columns
    - column types
    - nullability

    Future:
    - indexes
    - constraints
    - rename detection
    - FK diffs
    """

    def __init__(
        self,
        connection: Connection,
        metadata: MetaData,
        schemas: Set[str],
    ):
        self.connection = connection
        self.metadata = metadata
        self.schemas = schemas
        self.inspector = inspect(connection)

    # ==============================================================================================
    # Public API
    # ==============================================================================================

    def diff(self) -> SchemaDiff:
        result = SchemaDiff()

        # ------------------------------------------------------------------
        # Ensure schemas exist
        # ------------------------------------------------------------------
        existing_schemas = set(self.inspector.get_schema_names())

        for schema in sorted(self.schemas):
            if schema not in existing_schemas:
                result.add(CreateSchemaOp(schema=schema))

        # ------------------------------------------------------------------
        # Compare tables
        # ------------------------------------------------------------------
        metadata_tables = self._group_metadata_tables()

        for schema, tables in metadata_tables.items():

            db_tables = set(self.inspector.get_table_names(schema=schema))

            # --------------------------------------------------------------
            # Missing tables
            # --------------------------------------------------------------
            for table_name, table in tables.items():

                if table_name not in db_tables:
                    create_op = self._build_create_table_op(table)

                    result.add(create_op)

                    continue

                # Existing table → compare structure
                self._diff_existing_table(
                    result=result,
                    table=table,
                )

            # --------------------------------------------------------------
            # Extra tables
            # --------------------------------------------------------------
            metadata_names = set(tables.keys())

            extra_tables = db_tables - metadata_names

            for table_name in sorted(extra_tables):
                result.add(
                    DropTableOp(
                        schema=schema,
                        table=table_name,
                    )
                )

        return result

    # ==============================================================================================
    # Metadata helpers
    # ==============================================================================================

    def _group_metadata_tables(self) -> Dict[str, Dict[str, Table]]:
        grouped: Dict[str, Dict[str, Table]] = {}

        for table in self.metadata.tables.values():

            schema = table.schema

            if schema not in self.schemas:
                continue

            grouped.setdefault(schema, {})
            grouped[schema][table.name] = table

        return grouped

    # ==============================================================================================
    # Table creation
    # ==============================================================================================

    def _build_create_table_op(self, table: Table) -> CreateTableOp:
        columns = tuple(col.copy() for col in table.columns)
        if table.schema is None:
            raise RuntimeError(f"Unscoped table detected: {table.name}")
        return CreateTableOp(
            schema=table.schema,
            table=table.name,
            columns=columns,
        )

    # ==============================================================================================
    # Existing table diff
    # ==============================================================================================

    def _diff_existing_table(
        self,
        result: SchemaDiff,
        table: Table,
    ) -> None:
        if table.schema is None:
            raise RuntimeError(f"Unscoped table detected: {table.name}")

        schema = table.schema
        table_name = table.name

        db_columns_info = {
            col["name"]: col
            for col in self.inspector.get_columns(
                table_name,
                schema=schema,
            )
        }

        metadata_columns = {col.name: col for col in table.columns}

        db_column_names = set(db_columns_info.keys())
        metadata_column_names = set(metadata_columns.keys())

        # ------------------------------------------------------------------------------------------
        # Missing columns
        # ------------------------------------------------------------------------------------------
        missing_columns = metadata_column_names - db_column_names

        for col_name in sorted(missing_columns):

            column = metadata_columns[col_name]

            result.add(
                AddColumnOp(
                    schema=schema,
                    table=table_name,
                    column_name=col_name,
                    column_def=column.copy(),
                )
            )

        # ------------------------------------------------------------------------------------------
        # Extra columns
        # ------------------------------------------------------------------------------------------
        extra_columns = db_column_names - metadata_column_names

        for col_name in sorted(extra_columns):

            result.add(
                DropColumnOp(
                    schema=schema,
                    table=table_name,
                    column_name=col_name,
                )
            )

        # ------------------------------------------------------------------------------------------
        # Existing columns
        # ------------------------------------------------------------------------------------------
        shared_columns = db_column_names & metadata_column_names

        for col_name in sorted(shared_columns):

            db_col = db_columns_info[col_name]
            meta_col = metadata_columns[col_name]

            self._diff_column(
                result=result,
                schema=schema,
                table=table_name,
                db_col=db_col,
                meta_col=meta_col,
            )

    # ==============================================================================================
    # Column diff
    # ==============================================================================================

    def _diff_column(
        self,
        result: SchemaDiff,
        schema: str,
        table: str,
        db_col: ReflectedColumn,
        meta_col,
    ) -> None:

        col_name = meta_col.name

        # ------------------------------------------------------------------
        # Type comparison
        # ------------------------------------------------------------------
        db_type = db_col["type"].compile(dialect=self.connection.dialect)

        meta_type = meta_col.type.compile(dialect=self.connection.dialect)

        if db_type != meta_type:

            log_d(
                mod,
                f"Type mismatch {schema}.{table}.{col_name}: " f"{db_type} -> {meta_type}",
            )

            result.add(
                AlterColumnTypeOp(
                    schema=schema,
                    table=table,
                    column=col_name,
                    old_type=db_col["type"],
                    new_type=meta_col.type,
                )
            )

        # ------------------------------------------------------------------
        # Nullability comparison
        # ------------------------------------------------------------------
        db_nullable = db_col["nullable"]
        meta_nullable = meta_col.nullable

        if db_nullable != meta_nullable:

            log_d(
                mod,
                f"Nullability mismatch {schema}.{table}.{col_name}: " f"{db_nullable} -> {meta_nullable}",
            )

            result.add(
                AlterColumnNullabilityOp(
                    schema=schema,
                    table=table,
                    column=col_name,
                    nullable=meta_nullable,
                )
            )
