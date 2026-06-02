# kronicle/db/migration/schema_diff_engine.py
from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy import inspect
from sqlalchemy.engine import Connection
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.schema import MetaData, Table

from kronicle.db.migration.operations import (
    AddCheckConstraintOp,
    AddColumnOp,
    AddForeignKeyOp,
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

            metadata_names = set(tables.keys())

            # Tables in metadata but missing from DB → create candidates
            missing_tables = metadata_names - db_tables
            # Tables in DB but absent from metadata → drop / rename candidates
            extra_tables = db_tables - metadata_names

            # --------------------------------------------------------------
            # Rename detection (avoid data-lossy drop+create)
            # --------------------------------------------------------------
            matched = self._detect_table_renames(schema, missing_tables, extra_tables)
            for old_name, new_name in matched:
                log_w(mod, f"Rename detected: {schema}.{old_name} -> {new_name}")
                result.add(RenameTableOp(schema=schema, old_name=old_name, new_name=new_name))
                missing_tables.discard(new_name)
                extra_tables.discard(old_name)

            # --------------------------------------------------------------
            # Remaining missing tables → create
            # --------------------------------------------------------------
            for table_name in sorted(missing_tables):
                table = tables[table_name]
                result.add(self._build_create_table_op(table))

            # --------------------------------------------------------------
            # Existing tables → compare structure
            # --------------------------------------------------------------
            for table_name in sorted(metadata_names & db_tables):
                self._diff_existing_table(result=result, table=tables[table_name])

            # --------------------------------------------------------------
            # Remaining extra tables → drop
            # --------------------------------------------------------------
            for table_name in sorted(extra_tables):
                result.add(DropTableOp(schema=schema, table=table_name))

        return result

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
        import difflib

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
        import difflib

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
        # Column rename detection + add / drop
        # ------------------------------------------------------------------------------------------
        missing_columns = metadata_column_names - db_column_names
        extra_columns = db_column_names - metadata_column_names

        matched = self._detect_column_renames(table_name, missing_columns, extra_columns)
        for old_name, new_name in matched:
            log_w(mod, f"Column rename detected: {schema}.{table_name}.{old_name} -> {new_name}")
            result.add(RenameColumnOp(schema=schema, table=table_name, old_name=old_name, new_name=new_name))
            missing_columns.discard(new_name)
            extra_columns.discard(old_name)

        for col_name in sorted(missing_columns):
            result.add(
                AddColumnOp(
                    schema=schema,
                    table=table_name,
                    column_name=col_name,
                    column_def=metadata_columns[col_name].copy(),
                )
            )

        for col_name in sorted(extra_columns):
            result.add(
                DropColumnOp(
                    schema=schema,
                    table=table_name,
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
                table=table_name,
                db_col=db_col,
                meta_col=meta_col,
            )

        # ------------------------------------------------------------------------------------------
        # Indexes diff
        # ------------------------------------------------------------------------------------------
        self._diff_indexes(result=result, schema=schema, table=table, table_name=table_name)

        # ------------------------------------------------------------------------------------------
        # Constraints diff (unique, check, FK)
        # ------------------------------------------------------------------------------------------
        self._diff_constraints(result=result, schema=schema, table=table, table_name=table_name)

        # ------------------------------------------------------------------------------------------
        # Deduplication: DropIndexOps backed by a DropConstraintOp
        # On PostgreSQL, DROP CONSTRAINT on a unique index drops the backing index automatically.
        # A subsequent DropIndexOp for the same name would fail — remove it.
        # ------------------------------------------------------------------------------------------
        drop_constraint_names = {
            op.constraint_name
            for op in result.operations
            if isinstance(op, DropConstraintOp) and op.table == table_name and op.schema == schema
        }
        result.operations = [
            op
            for op in result.operations
            if not (
                isinstance(op, DropIndexOp)
                and op.table == table_name
                and op.schema == schema
                and op.index_name in drop_constraint_names
            )
        ]

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
        table_name: str,
    ) -> None:
        db_indexes = {
            idx["name"]: idx
            for idx in self.inspector.get_indexes(table_name, schema=schema)
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
                    table=table_name,
                    index_name=name,
                    column_names=tuple(col.name for col in idx.columns),
                    unique=idx.unique,
                )
            )

        for name in sorted(db_names - meta_names):
            result.add(DropIndexOp(schema=schema, table=table_name, index_name=name))

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
        import difflib

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

    def _diff_constraints(
        self,
        result: SchemaDiff,
        schema: str,
        table: Table,
        table_name: str,
    ) -> None:
        from sqlalchemy import CheckConstraint, ForeignKeyConstraint, UniqueConstraint

        # ------------------------------------------------------------------
        # Unique constraints
        # ------------------------------------------------------------------
        db_unique = {
            str(c["name"]): c for c in self.inspector.get_unique_constraints(table_name, schema=schema) if c.get("name")
        }
        meta_unique = {str(c.name): c for c in table.constraints if isinstance(c, UniqueConstraint) and c.name}

        db_uniq_names = set(db_unique.keys())
        meta_uniq_names = set(meta_unique.keys())

        matched = self._detect_constraint_renames(meta_uniq_names - db_uniq_names, db_uniq_names - meta_uniq_names)
        for old_name, new_name in matched:
            log_w(mod, f"Unique constraint rename detected: {schema}.{table_name}.{old_name} -> {new_name}")
            result.add(RenameConstraintOp(schema=schema, table=table_name, old_name=old_name, new_name=new_name))
            meta_uniq_names.discard(new_name)
            db_uniq_names.discard(old_name)

        for name in sorted(meta_uniq_names - db_uniq_names):
            cons = meta_unique[name]
            result.add(
                AddUniqueConstraintOp(
                    schema=schema,
                    table=table_name,
                    constraint_name=name,
                    columns=tuple(col.name for col in cons.columns),
                )
            )

        for name in sorted(db_uniq_names - meta_uniq_names):
            result.add(DropConstraintOp(schema=schema, table=table_name, constraint_name=name))

        # ------------------------------------------------------------------
        # Check constraints
        # ------------------------------------------------------------------
        db_check = {
            str(c["name"]): c for c in self.inspector.get_check_constraints(table_name, schema=schema) if c.get("name")
        }
        meta_check = {str(c.name): c for c in table.constraints if isinstance(c, CheckConstraint) and c.name}

        db_chk_names = set(db_check.keys())
        meta_chk_names = set(meta_check.keys())

        matched = self._detect_constraint_renames(meta_chk_names - db_chk_names, db_chk_names - meta_chk_names)
        for old_name, new_name in matched:
            log_w(mod, f"Check constraint rename detected: {schema}.{table_name}.{old_name} -> {new_name}")
            result.add(RenameConstraintOp(schema=schema, table=table_name, old_name=old_name, new_name=new_name))
            meta_chk_names.discard(new_name)
            db_chk_names.discard(old_name)

        for name in sorted(meta_chk_names - db_chk_names):
            cons = meta_check[name]
            result.add(
                AddCheckConstraintOp(
                    schema=schema,
                    table=table_name,
                    constraint_name=name,
                    sqltext=str(cons.sqltext),
                )
            )

        for name in sorted(db_chk_names - meta_chk_names):
            result.add(DropConstraintOp(schema=schema, table=table_name, constraint_name=name))

        # ------------------------------------------------------------------
        # Foreign key constraints
        # ------------------------------------------------------------------
        db_fk = {str(c["name"]): c for c in self.inspector.get_foreign_keys(table_name, schema=schema) if c.get("name")}
        meta_fk = {str(c.name): c for c in table.constraints if isinstance(c, ForeignKeyConstraint) and c.name}

        db_fk_names = set(db_fk.keys())
        meta_fk_names = set(meta_fk.keys())

        matched = self._detect_constraint_renames(meta_fk_names - db_fk_names, db_fk_names - meta_fk_names)
        for old_name, new_name in matched:
            log_w(mod, f"FK constraint rename detected: {schema}.{table_name}.{old_name} -> {new_name}")
            result.add(RenameConstraintOp(schema=schema, table=table_name, old_name=old_name, new_name=new_name))
            meta_fk_names.discard(new_name)
            db_fk_names.discard(old_name)

        for name in sorted(meta_fk_names - db_fk_names):
            cons = meta_fk[name]
            elements = list(cons.elements)
            result.add(
                AddForeignKeyOp(
                    schema=schema,
                    table=table_name,
                    constraint_name=name,
                    referred_table=elements[0].column.table.name,
                    local_columns=tuple(col.name for col in cons.columns),
                    referred_columns=tuple(elem.column.name for elem in elements),
                    ondelete=cons.ondelete,
                    onupdate=cons.onupdate,
                )
            )

        for name in sorted(db_fk_names - meta_fk_names):
            result.add(DropForeignKeyOp(schema=schema, table=table_name, constraint_name=name))
