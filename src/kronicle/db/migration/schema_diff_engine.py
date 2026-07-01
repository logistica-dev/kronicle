# kronicle/db/migration/schema_diff_engine.py
from __future__ import annotations

import difflib
from dataclasses import dataclass, field

from sqlalchemy import CheckConstraint, ForeignKeyConstraint, UniqueConstraint, inspect
from sqlalchemy.engine import Connection
from sqlalchemy.engine.interfaces import ReflectedColumn, ReflectedForeignKeyConstraint
from sqlalchemy.schema import MetaData, Table

from kronicle.db.migration.operations import (
    AddCheckConstraintOp,
    AddColumnOp,
    AddForeignKeyOp,
    AddNonNullableColumnOp,
    AddPrimaryKeyOp,
    AddUniqueConstraintOp,
    AlterColumnNullabilityOp,
    AlterColumnTypeOp,
    CreateIndexOp,
    CreateSchemaOp,
    CreateTableOp,
    DbStructureOperation,
    DropColumnOp,
    DropConstraintOp,
    DropForeignKeyOp,
    DropIndexOp,
    DropPrimaryKeyOp,
    DropTableOp,
    RenameColumnOp,
    RenameConstraintOp,
    RenameTableOp,
)
from kronicle.utils.dev_logs import log_d, log_w

mod = "schema_diff"


# ==================================================================================================
# Diff result
# ==================================================================================================


@dataclass
class SchemaDiff:
    operations: list[DbStructureOperation] = field(default_factory=list)

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
    - indexes
    - constraints (unique, check, FK)
    """

    def __init__(
        self,
        connection: Connection,
        metadata: MetaData,
        schemas: set[str],
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
        self._diff_schemas(result)
        for schema, tables in self._group_metadata_tables().items():
            self._diff_schema_tables(result, schema, tables)
        return result

    def _diff_schemas(self, result: SchemaDiff) -> None:
        existing_schemas = set(self.inspector.get_schema_names())
        for schema in sorted(self.schemas):
            if schema not in existing_schemas:
                result.add(CreateSchemaOp(schema=schema))

    def _diff_schema_tables(self, result: SchemaDiff, schema: str, tables: dict[str, Table]) -> None:
        db_tables = set(self.inspector.get_table_names(schema=schema))
        metadata_names = set(tables.keys())
        missing_tables = metadata_names - db_tables
        extra_tables = db_tables - metadata_names

        matched = self._detect_table_renames(schema, missing_tables, extra_tables)
        for old_name, new_name in matched:
            log_w(mod, f"Rename detected: {schema}.{old_name} -> {new_name}")
            result.add(RenameTableOp(schema=schema, old_name=old_name, new_name=new_name))
            missing_tables.discard(new_name)
            extra_tables.discard(old_name)

        self._create_missing_tables(result, schema, tables, missing_tables)
        self._diff_tables_in_common(result, schema, tables, metadata_names, db_tables, matched)
        self._drop_extra_tables(result, schema, extra_tables)

    def _create_missing_tables(
        self, result: SchemaDiff, schema: str, tables: dict[str, Table], missing_tables: set[str]
    ) -> None:
        for table_name in sorted(missing_tables):
            table = tables[table_name]
            result.add(self._build_create_table_op(table))
            for idx in table.indexes:
                if not idx.name or not idx.columns:
                    continue
                result.add(
                    CreateIndexOp(
                        schema=schema,
                        table=table_name,
                        index_name=idx.name,
                        column_names=tuple(col.name for col in idx.columns),
                        unique=idx.unique,
                    )
                )
            for cons in table.constraints:
                if isinstance(cons, UniqueConstraint) and cons.name:
                    result.add(
                        AddUniqueConstraintOp(
                            schema=schema,
                            table=table_name,
                            constraint_name=str(cons.name),
                            columns=tuple(col.name for col in cons.columns),
                        )
                    )

    def _diff_tables_in_common(
        self,
        result: SchemaDiff,
        schema: str,
        tables: dict[str, Table],
        metadata_names: set[str],
        db_tables: set[str],
        matched: list[tuple[str, str]],
    ) -> None:
        for table_name in sorted(metadata_names & db_tables):
            self._diff_existing_table(result=result, table=tables[table_name])
        for old_name, new_name in matched:
            self._diff_existing_table(result=result, table=tables[new_name], db_table_name=old_name)

    def _drop_extra_tables(self, result: SchemaDiff, schema: str, extra_tables: set[str]) -> None:
        for table_name in sorted(extra_tables):
            result.add(DropTableOp(schema=schema, table=table_name))

    # ==============================================================================================
    # Metadata helpers
    # ==============================================================================================

    def _group_metadata_tables(self) -> dict[str, dict[str, Table]]:
        grouped: dict[str, dict[str, Table]] = {}

        for table in self.metadata.tables.values():

            schema = table.schema

            if schema not in self.schemas:
                continue

            grouped.setdefault(schema, {})
            grouped[schema][table.name] = table

        return grouped

    # ==============================================================================================
    # Rename detection
    # ==============================================================================================

    @staticmethod
    def _detect_table_renames(
        schema: str,
        missing_tables: set[str],
        extra_tables: set[str],
    ) -> list[tuple[str, str]]:
        """
        Detect potential table renames using string similarity
        (e.g. channel -> channels, zones_hierarchy -> zone_hierarchy).

        Returns list of (old_name, new_name) pairs.
        """
        renames: list[tuple[str, str]] = []
        used_extras: set[str] = set()

        for missing in sorted(missing_tables):
            best_match: str | None = None
            best_ratio = 0.0

            for extra in extra_tables:
                if extra in used_extras:
                    continue
                ratio = difflib.SequenceMatcher(None, extra, missing).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_match = extra

            if best_match is not None and best_ratio >= 0.5:
                renames.append((best_match, missing))
                used_extras.add(best_match)

        return renames

    # ==============================================================================================
    # Column rename detection
    # ==============================================================================================

    @staticmethod
    def _detect_column_renames(
        table_name: str,
        missing_columns: set[str],
        extra_columns: set[str],
    ) -> list[tuple[str, str]]:
        """
        Detect potential column renames via string similarity
        (e.g. temp -> temperature, decription -> description).

        Returns list of (old_name, new_name) pairs.
        """
        renames: list[tuple[str, str]] = []
        used_extras: set[str] = set()

        for missing in sorted(missing_columns):
            best_match: str | None = None
            best_ratio = 0.0

            for extra in extra_columns:
                if extra in used_extras:
                    continue
                ratio = difflib.SequenceMatcher(None, extra, missing).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_match = extra

            if best_match is not None and best_ratio >= 0.5:
                renames.append((best_match, missing))
                used_extras.add(best_match)

        return renames

    # ==============================================================================================
    # Table creation
    # ==============================================================================================

    def _build_create_table_op(self, table: Table) -> CreateTableOp:
        columns = tuple(col.copy() for col in table.columns)
        # Strip unique=True from column copies – the constraint is emitted
        # separately as AddUniqueConstraintOp with the correct explicit name.
        for col in columns:
            col.unique = False
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
        db_table_name: str | None = None,
    ) -> None:
        if table.schema is None:
            raise RuntimeError(f"Unscoped table detected: {table.name}")

        schema = table.schema
        db_table = db_table_name or table.name
        target_table = table.name  # operations target the final name (rename runs first)

        db_columns_info = {
            col["name"]: col
            for col in self.inspector.get_columns(
                db_table,
                schema=schema,
            )
        }

        metadata_columns = {col.name: col for col in table.columns}

        db_column_names = set(db_columns_info.keys())
        metadata_column_names = set(metadata_columns.keys())

        # ------------------------------------------------------------------------------------------
        # Column rename detection + add / drop
        # ------------------------------------------------------------------------------------------
        missing_columns = metadata_column_names - db_column_names
        extra_columns = db_column_names - metadata_column_names

        matched = self._detect_column_renames(target_table, missing_columns, extra_columns)
        for old_name, new_name in matched:
            log_w(mod, f"Column rename detected: {schema}.{target_table}.{old_name} -> {new_name}")
            result.add(RenameColumnOp(schema=schema, table=target_table, old_name=old_name, new_name=new_name))
            missing_columns.discard(new_name)
            extra_columns.discard(old_name)

        # ------------------------------------------------------------------------------------------
        # Primary key diff — drop old PK before column drops so columns
        # that are no longer part of the PK can be dropped safely.
        # ------------------------------------------------------------------------------------------
        self._diff_primary_key(result, schema, target_table, db_table, table)

        for col_name in sorted(missing_columns):
            col_def = metadata_columns[col_name]
            op_cls = (
                AddNonNullableColumnOp
                if col_def.nullable is False and col_def.server_default is None and col_def.default is None
                else AddColumnOp
            )
            result.add(
                op_cls(
                    schema=schema,
                    table=target_table,
                    column_name=col_name,
                    column_def=col_def.copy(),
                )
            )

        for col_name in sorted(extra_columns):
            result.add(
                DropColumnOp(
                    schema=schema,
                    table=target_table,
                    column_name=col_name,
                )
            )

        # ------------------------------------------------------------------------------------------
        # Existing columns → diff type / nullability
        # ------------------------------------------------------------------------------------------
        shared_columns = db_column_names & metadata_column_names

        for col_name in sorted(shared_columns):

            db_col = db_columns_info[col_name]
            meta_col = metadata_columns[col_name]

            self._diff_column(
                result=result,
                schema=schema,
                table=target_table,
                db_col=db_col,
                meta_col=meta_col,
            )

        # ------------------------------------------------------------------------------------------
        # Indexes diff
        # ------------------------------------------------------------------------------------------
        self._diff_indexes(result=result, schema=schema, table=table, db_table=db_table, target_table=target_table)

        # ------------------------------------------------------------------------------------------
        # Constraints diff (unique, check, FK)
        # ------------------------------------------------------------------------------------------
        self._diff_constraints(result=result, schema=schema, table=table, db_table=db_table, target_table=target_table)

        # ------------------------------------------------------------------------------------------
        # Deduplication: DropIndexOps backed by a DropConstraintOp
        # On PostgreSQL, DROP CONSTRAINT on a unique index drops the backing index automatically.
        # A subsequent DropIndexOp for the same name would fail — remove it.
        # ------------------------------------------------------------------------------------------
        drop_constraint_names = {
            op.constraint_name
            for op in result.operations
            if isinstance(op, DropConstraintOp) and op.table == target_table and op.schema == schema
        }
        result.operations = [
            op
            for op in result.operations
            if not (
                isinstance(op, DropIndexOp)
                and op.table == target_table
                and op.schema == schema
                and op.index_name in drop_constraint_names
            )
        ]

    # ==============================================================================================
    # Primary key diff
    # ==============================================================================================

    def _diff_primary_key(
        self,
        result: SchemaDiff,
        schema: str,
        target_table: str,
        db_table: str,
        table: Table,
    ) -> None:
        db_pk_info = self.inspector.get_pk_constraint(db_table, schema=schema)
        db_pk_columns = set(db_pk_info.get("constrained_columns", []))
        db_pk_name = db_pk_info.get("name") or f"{db_table}_pkey"

        model_pk_columns = {col.name for col in table.primary_key.columns}

        if db_pk_columns == model_pk_columns:
            return

        if db_pk_columns:
            result.add(
                DropPrimaryKeyOp(
                    schema=schema,
                    table=target_table,
                    constraint_name=db_pk_name,
                )
            )

        if model_pk_columns:
            # Use the model PK constraint name if defined, otherwise construct one
            model_pk_name = table.primary_key.name or f"{target_table}_pkey"
            result.add(
                AddPrimaryKeyOp(
                    schema=schema,
                    table=target_table,
                    constraint_name=str(model_pk_name),
                    columns=tuple(sorted(model_pk_columns)),
                )
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

    # ==============================================================================================
    # Indexes diff
    # ==============================================================================================

    def _diff_indexes(
        self,
        result: SchemaDiff,
        schema: str,
        table: Table,
        db_table: str,
        target_table: str,
    ) -> None:
        db_indexes = {
            idx["name"]: idx
            for idx in self.inspector.get_indexes(db_table, schema=schema)
            if (
                idx
                and idx["name"]
                and not idx["name"].endswith("_pkey")  # skip auto-generated PK indexes
                and not idx.get("duplicates_constraint")  # managed by _diff_constraints
            )
        }
        meta_indexes = {str(idx.name): idx for idx in table.indexes if idx.name is not None}

        db_names = set(db_indexes.keys())
        meta_names = set(meta_indexes.keys())

        for name in sorted(meta_names - db_names):
            idx = meta_indexes[name]
            if not idx.columns:
                continue
            result.add(
                CreateIndexOp(
                    schema=schema,
                    table=target_table,
                    index_name=name,
                    column_names=tuple(col.name for col in idx.columns),
                    unique=idx.unique,
                )
            )

        for name in sorted(db_names - meta_names):
            result.add(DropIndexOp(schema=schema, table=target_table, index_name=name))

    # ==============================================================================================
    # Constraints diff (unique, check, FK)
    # ==============================================================================================

    @staticmethod
    def _detect_constraint_renames(
        missing: set[str],
        extra: set[str],
    ) -> list[tuple[str, str]]:
        """
        Detect potential constraint renames via string similarity
        (e.g. uq_channel_name -> uq_channels_name).

        Returns list of (old_name, new_name) pairs.
        """

        renames: list[tuple[str, str]] = []
        used: set[str] = set()

        for m in sorted(missing):
            best: str | None = None
            best_ratio = 0.0
            for e in extra:
                if e in used:
                    continue
                ratio = difflib.SequenceMatcher(None, e, m).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
                    best = e
            if best is not None and best_ratio >= 0.5:
                renames.append((best, m))
                used.add(best)

        return renames

    def _diff_unique_constraints(
        self,
        result: SchemaDiff,
        schema: str,
        table: Table,
        db_table: str,
        target_table: str,
    ) -> None:
        db_unique = {
            str(c["name"]): c for c in self.inspector.get_unique_constraints(db_table, schema=schema) if c.get("name")
        }
        meta_unique = {}
        for c in table.constraints:
            if isinstance(c, UniqueConstraint):
                name = str(c.name) if c.name else f"{table.name}_{'_'.join(col.name for col in c.columns)}_key"
                meta_unique[name] = c

        db_uniq_names = set(db_unique.keys())
        meta_uniq_names = set(meta_unique.keys())

        matched = self._detect_constraint_renames(meta_uniq_names - db_uniq_names, db_uniq_names - meta_uniq_names)
        for old_name, new_name in matched:
            log_w(mod, f"Unique constraint rename detected: {schema}.{target_table}.{old_name} -> {new_name}")
            result.add(RenameConstraintOp(schema=schema, table=target_table, old_name=old_name, new_name=new_name))
            meta_uniq_names.discard(new_name)
            db_uniq_names.discard(old_name)

        for name in sorted(meta_uniq_names - db_uniq_names):
            cons = meta_unique[name]
            result.add(
                AddUniqueConstraintOp(
                    schema=schema,
                    table=target_table,
                    constraint_name=name,
                    columns=tuple(col.name for col in cons.columns),
                )
            )

        for name in sorted(db_uniq_names - meta_uniq_names):
            result.add(DropConstraintOp(schema=schema, table=target_table, constraint_name=name))

    def _diff_check_constraints(
        self,
        result: SchemaDiff,
        schema: str,
        table: Table,
        db_table: str,
        target_table: str,
    ) -> None:
        db_check = {
            str(c["name"]): c for c in self.inspector.get_check_constraints(db_table, schema=schema) if c.get("name")
        }
        meta_check = {str(c.name): c for c in table.constraints if isinstance(c, CheckConstraint) and c.name}

        db_chk_names = set(db_check.keys())
        meta_chk_names = set(meta_check.keys())

        matched = self._detect_constraint_renames(meta_chk_names - db_chk_names, db_chk_names - meta_chk_names)
        for old_name, new_name in matched:
            log_w(mod, f"Check constraint rename detected: {schema}.{target_table}.{old_name} -> {new_name}")
            result.add(RenameConstraintOp(schema=schema, table=target_table, old_name=old_name, new_name=new_name))
            meta_chk_names.discard(new_name)
            db_chk_names.discard(old_name)

        for name in sorted(meta_chk_names - db_chk_names):
            cons = meta_check[name]
            result.add(
                AddCheckConstraintOp(
                    schema=schema,
                    table=target_table,
                    constraint_name=name,
                    sqltext=str(cons.sqltext),
                )
            )

        for name in sorted(db_chk_names - meta_chk_names):
            result.add(DropConstraintOp(schema=schema, table=target_table, constraint_name=name))

    @staticmethod
    def _fk_meta_key(constraint: ForeignKeyConstraint) -> tuple:
        """Build a stable key for a metadata FK constraint, using columns when name is absent."""
        name = constraint.name
        if name:
            return ("name", str(name))
        cols = tuple(col.name for col in constraint.columns)
        elements = list(constraint.elements)
        referred_table = elements[0].column.table.name if elements and elements[0].column is not None else None
        referred_cols = tuple(elem.column.name for elem in elements)
        return ("cols", cols, referred_table, referred_cols)

    @staticmethod
    def _fk_db_key(fk_def: ReflectedForeignKeyConstraint) -> tuple:
        """Build a stable key for a DB FK definition, always based on columns + referred table."""
        cols = tuple(fk_def.get("constrained_columns", ()))
        referred_table = fk_def.get("referred_table")
        referred_cols = tuple(fk_def.get("referred_columns", ()))
        return ("cols", cols, referred_table, referred_cols)

    def _build_fk_dicts(
        self, schema: str, db_table: str, table: Table
    ) -> tuple[dict[tuple, ReflectedForeignKeyConstraint], dict[tuple, ForeignKeyConstraint]]:
        db_fk: dict[tuple, ReflectedForeignKeyConstraint] = {}
        for c in self.inspector.get_foreign_keys(db_table, schema=schema):
            db_fk[self._fk_db_key(c)] = c

        meta_fk: dict[tuple, ForeignKeyConstraint] = {}
        for c in table.constraints:
            if isinstance(c, ForeignKeyConstraint):
                meta_fk[self._fk_meta_key(c)] = c

        return db_fk, meta_fk

    def _diff_fk_renames(
        self,
        result: SchemaDiff,
        schema: str,
        target_table: str,
        db_fk: dict[tuple, ReflectedForeignKeyConstraint],
        meta_fk: dict[tuple, ForeignKeyConstraint],
        db_fk_keys: set[tuple],
        meta_fk_keys: set[tuple],
    ) -> None:
        db_fk_named = {k for k in db_fk if k[0] == "name"}
        meta_fk_named = {k for k in meta_fk if k[0] == "name"}
        db_named = {k[1] for k in db_fk_named}
        meta_named = {k[1] for k in meta_fk_named}

        matched = self._detect_constraint_renames(meta_named - db_named, db_named - meta_named)
        for old_name, new_name in matched:
            log_w(mod, f"FK constraint rename detected: {schema}.{target_table}.{old_name} -> {new_name}")
            old_key = ("name", old_name)
            new_key = ("name", new_name)
            result.add(RenameConstraintOp(schema=schema, table=target_table, old_name=old_name, new_name=new_name))
            meta_fk_keys.discard(new_key)
            db_fk_keys.discard(old_key)

    def _diff_fk_additions(
        self,
        result: SchemaDiff,
        schema: str,
        target_table: str,
        meta_fk: dict[tuple, ForeignKeyConstraint],
        keys: set[tuple],
    ) -> None:
        for key in sorted(keys, key=str):
            cons = meta_fk[key]
            elements = list(cons.elements)
            if not elements or elements[0].column is None:
                continue
            fk_name: str | None = cons.name if isinstance(cons.name, str) else None
            name = fk_name or f"{target_table}_{'_'.join(col.name for col in cons.columns)}_fkey"
            result.add(
                AddForeignKeyOp(
                    schema=schema,
                    table=target_table,
                    constraint_name=name,
                    referred_table=elements[0].column.table.name,
                    referred_schema=elements[0].column.table.schema,
                    local_columns=tuple(col.name for col in cons.columns),
                    referred_columns=tuple(elem.column.name for elem in elements),
                    ondelete=cons.ondelete,
                    onupdate=cons.onupdate,
                )
            )

    def _diff_fk_removals(
        self,
        result: SchemaDiff,
        schema: str,
        target_table: str,
        db_fk: dict[tuple, ReflectedForeignKeyConstraint],
        keys: set[tuple],
    ) -> None:
        for key in sorted(keys, key=str):
            fk_def = db_fk[key]
            name = fk_def.get("name")
            if name:
                result.add(DropForeignKeyOp(schema=schema, table=target_table, constraint_name=name))

    def _diff_fk_changes(
        self,
        result: SchemaDiff,
        schema: str,
        target_table: str,
        db_fk: dict[tuple, ReflectedForeignKeyConstraint],
        meta_fk: dict[tuple, ForeignKeyConstraint],
        keys: set[tuple],
    ) -> None:
        for key in sorted(keys, key=str):
            cons = meta_fk[key]
            db_def = db_fk[key]
            elements = list(cons.elements)
            if not elements or elements[0].column is None:
                continue

            meta_local_cols = tuple(col.name for col in cons.columns)
            meta_referred_cols = tuple(elem.column.name for elem in elements)
            meta_referred_table = elements[0].column.table.name

            db_local_cols = tuple(db_def.get("constrained_columns", ()))
            db_referred_cols = tuple(db_def.get("referred_columns", ()))
            db_referred_table = db_def.get("referred_table")
            db_ondelete = (db_def.get("options") or {}).get("ondelete")
            db_onupdate = (db_def.get("options") or {}).get("onupdate")
            meta_ondelete = cons.ondelete.upper() if cons.ondelete else None
            meta_onupdate = cons.onupdate.upper() if cons.onupdate else None
            db_ondelete = db_ondelete.upper() if db_ondelete else None
            db_onupdate = db_onupdate.upper() if db_onupdate else None

            if (
                meta_local_cols != db_local_cols
                or meta_referred_cols != db_referred_cols
                or meta_referred_table != db_referred_table
                or meta_ondelete != db_ondelete
                or meta_onupdate != db_onupdate
            ):
                db_name = db_def.get("name")
                fk_name: str | None = cons.name if isinstance(cons.name, str) else None
                cons_name = db_name or fk_name or f"{target_table}_{'_'.join(col.name for col in cons.columns)}_fkey"
                log_d(
                    mod,
                    f"FK definition changed {schema}.{target_table}.{cons_name}: "
                    f"ondelete {db_ondelete} -> {meta_ondelete}, onupdate {db_onupdate} -> {meta_onupdate}",
                )
                if db_name:
                    result.add(DropForeignKeyOp(schema=schema, table=target_table, constraint_name=db_name))
                result.add(
                    AddForeignKeyOp(
                        schema=schema,
                        table=target_table,
                        constraint_name=cons_name,
                        referred_table=meta_referred_table,
                        referred_schema=(
                            elements[0].column.table.schema if elements and elements[0].column is not None else None
                        ),
                        local_columns=meta_local_cols,
                        referred_columns=meta_referred_cols,
                        ondelete=cons.ondelete,
                        onupdate=cons.onupdate,
                    )
                )

    def _diff_foreign_key_constraints(
        self,
        result: SchemaDiff,
        schema: str,
        table: Table,
        db_table: str,
        target_table: str,
    ) -> None:
        db_fk, meta_fk = self._build_fk_dicts(schema, db_table, table)

        db_fk_keys = set(db_fk.keys())
        meta_fk_keys = set(meta_fk.keys())

        self._diff_fk_renames(result, schema, target_table, db_fk, meta_fk, db_fk_keys, meta_fk_keys)
        self._diff_fk_additions(result, schema, target_table, meta_fk, meta_fk_keys - db_fk_keys)
        self._diff_fk_removals(result, schema, target_table, db_fk, db_fk_keys - meta_fk_keys)
        self._diff_fk_changes(result, schema, target_table, db_fk, meta_fk, meta_fk_keys & db_fk_keys)

    def _diff_constraints(
        self,
        result: SchemaDiff,
        schema: str,
        table: Table,
        db_table: str,
        target_table: str,
    ) -> None:
        self._diff_unique_constraints(result, schema, table, db_table, target_table)
        self._diff_check_constraints(result, schema, table, db_table, target_table)
        self._diff_foreign_key_constraints(result, schema, table, db_table, target_table)
