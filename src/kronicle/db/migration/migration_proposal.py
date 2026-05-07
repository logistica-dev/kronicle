# kronicle/db/migration/migration_proposal.py
from __future__ import annotations

from functools import cached_property

from sqlalchemy import inspect
from sqlalchemy.engine import Connection

from kronicle.db.base.kronicle_base import Base
from kronicle.db.migration.operations import (
    AddColumnOp,
    CreateTableOp,
    DbStructureOperation,
    DropColumnOp,
    DropTableOp,
    SafetyLevel,
)
from kronicle.db.registry import get_migration_schemas
from kronicle.utils.dev_logs import log_e, log_i, log_w

mod = "migration_proposal"


class MigrationProposal:
    """
    Diff engine:
        SQLAlchemy metadata (desired state)
        vs PostgreSQL inspector (actual state)

    Output:
        List[SchemaOperation]
    """

    def __init__(self, connection: Connection):
        self.connection = connection
        self.schemas = get_migration_schemas()
        self._inspector = inspect(connection)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _log_operation(self, op: DbStructureOperation):
        msg = f"{op.safety.level[:4].upper()} -> {op.describe()}"

        if op.safety.level == SafetyLevel.SAFE:
            log_i("migration", msg)
        elif op.safety.level == SafetyLevel.WARNING:
            log_w("migration", msg)
        else:
            log_e("migration", msg)

    def iter_tables(self):
        for table in Base.metadata.tables.values():
            if table.schema in self.schemas:
                yield table

    @cached_property
    def grouped_db_tables(self):
        """
        {schema -> set(table_names)}
        """
        return {schema: set(self._inspector.get_table_names(schema=schema)) for schema in self.schemas}

    @cached_property
    def grouped_model_tables(self):
        """
        {schema -> set(table_names)}
        """
        grouped = {schema: set() for schema in self.schemas}

        for table in self.iter_tables():
            grouped[table.schema].add(table.name)

        return grouped

    def get_db_columns(self, table_name: str, schema: str):
        return {c["name"] for c in self._inspector.get_columns(table_name, schema=schema)}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def to_operations(self) -> list[DbStructureOperation]:
        ops: list[DbStructureOperation] = []

        db_tables = self.grouped_db_tables
        model_tables = self.grouped_model_tables

        # ----------------------------------------------------------
        # TABLE CREATION + COLUMN DIFF
        # ----------------------------------------------------------
        for table in self.iter_tables():
            schema = table.schema
            name = table.name

            if name not in db_tables[schema]:
                op = CreateTableOp(schema=schema, table=name, columns=tuple(table.columns))
                self._log_operation(op)
                ops.append(op)
                continue

            ops.extend(self._diff_columns(table, schema, name))

        # ----------------------------------------------------------
        # TABLE DROPS
        # ----------------------------------------------------------
        ops.extend(self._diff_dropped_tables(db_tables, model_tables))

        return ops

    # ------------------------------------------------------------------
    # Column diff
    # ------------------------------------------------------------------

    def _diff_columns(self, table, schema: str, name: str) -> list[DbStructureOperation]:
        ops: list[DbStructureOperation] = []

        db_columns = self.get_db_columns(name, schema)
        model_columns = set(table.columns.keys())

        # ADD columns
        for col_name in model_columns - db_columns:
            col = table.columns[col_name]
            op = AddColumnOp(schema=schema, table=name, column_name=col_name, column_def=col)
            self._log_operation(op)
            ops.append(op)

        # DROP columns
        for col_name in db_columns - model_columns:
            op = DropColumnOp(schema=schema, table=name, column_name=col_name)
            self._log_operation(op)
            ops.append(op)

        return ops

    # ------------------------------------------------------------------
    # Table diff (drops only for now)
    # ------------------------------------------------------------------

    def _diff_dropped_tables(
        self,
        db_tables: dict[str, set[str]],
        model_tables: dict[str, set[str]],
    ) -> list[DbStructureOperation]:

        ops: list[DbStructureOperation] = []

        for schema, db_set in db_tables.items():
            model_set = model_tables.get(schema, set())

            for extra_table in db_set - model_set:
                op = DropTableOp(schema=schema, table=extra_table)
                self._log_operation(op)
                ops.append(op)

        return ops
