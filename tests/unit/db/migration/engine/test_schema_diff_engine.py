# tests/unit/db/migration/engine/test_schema_diff_engine.py
"""Unit tests for kronicle.db.migration.engine.schema_diff_engine.SchemaDiffEngine.

The engine reflects the live database through a SQLAlchemy Inspector. To keep
the tests hermetic we substitute a FakeInspector and patch the ``inspect``
factory imported by the module under test (``schema_diff_engine.inspect``).
"""

from typing import TypeVar, cast
from unittest.mock import patch

import pytest
from sqlalchemy import (
    CheckConstraint,
    Column,
    Connection,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.engine.interfaces import ReflectedForeignKeyConstraint

from kronicle.db.migration.engine.operations import (
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
    DropViewOp,
    RenameColumnOp,
    RenameConstraintOp,
    RenameTableOp,
)
from kronicle.db.migration.engine.schema_diff_engine import SchemaDiff, SchemaDiffEngine

TOp = TypeVar("TOp", bound=DbStructureOperation)


# ==================================================================================================
# FakeInspector
# ==================================================================================================


class FakeInspector:
    """In-memory stand-in for a SQLAlchemy Inspector."""

    def __init__(self, schemas=()):
        self._schemas = set(schemas)
        self._tables = {}
        self._views = {}
        self._columns = {}
        self._pk = {}
        self._indexes = {}
        self._unique = {}
        self._checks = {}
        self._fks = {}

    def add_table(self, schema, table, *, pk=None, columns=None, indexes=None, uniques=None, checks=None, fks=None):
        self._schemas.add(schema)
        self._tables.setdefault(schema, set()).add(table)
        self._columns[(schema, table)] = list(columns or [])
        self._pk[(schema, table)] = {"name": None, "constrained_columns": []} if pk is None else pk
        self._indexes[(schema, table)] = list(indexes or [])
        self._unique[(schema, table)] = list(uniques or [])
        self._checks[(schema, table)] = list(checks or [])
        self._fks[(schema, table)] = list(fks or [])

    def add_view(self, schema, view):
        self._schemas.add(schema)
        self._views.setdefault(schema, set()).add(view)

    def get_schema_names(self):
        return sorted(self._schemas)

    def get_table_names(self, schema=None):
        return sorted(self._tables.get(schema, set()))

    def get_view_names(self, schema=None):
        return sorted(self._views.get(schema, set()))

    def get_columns(self, table, schema=None):
        return list(self._columns.get((schema, table), []))

    def get_pk_constraint(self, table, schema=None):
        return dict(self._pk.get((schema, table), {"name": None, "constrained_columns": []}))

    def get_indexes(self, table, schema=None):
        return list(self._indexes.get((schema, table), []))

    def get_unique_constraints(self, table, schema=None):
        return list(self._unique.get((schema, table), []))

    def get_check_constraints(self, table, schema=None):
        return list(self._checks.get((schema, table), []))

    def get_foreign_keys(self, table, schema=None):
        return list(self._fks.get((schema, table), []))


# ==================================================================================================
# Helpers
# ==================================================================================================


class FakeConnection:
    """Bare connection-ish object carrying a real (sqlite) dialect for type compilation."""

    def __init__(self):
        engine = create_engine("sqlite://")
        self.dialect = engine.dialect


def _connection() -> FakeConnection:
    return FakeConnection()


def _diff(metadata, schemas, inspector):
    with patch("kronicle.db.migration.engine.schema_diff_engine.inspect", return_value=inspector):
        engine = SchemaDiffEngine(cast(Connection, _connection()), metadata, schemas)
        return engine.diff()


def _ops(result, op_type: type[TOp]) -> list[TOp]:
    return [op for op in result.operations if isinstance(op, op_type)]


def _col(name, type_, nullable=True):
    return {"name": name, "type": type_, "nullable": nullable, "default": None, "autoincrement": False}


def _users_table(metadata, *, with_email_constraint=True, extra_columns=()):
    args = [UniqueConstraint("email", name="uq_users_email")] if with_email_constraint else []
    return Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("email", String(120)),
        *extra_columns,
        *args,
        schema="public",
    )


def _users_in_db(inspector, *, email_type=None, email_nullable=True, extra_columns=()):
    if email_type is None:
        email_type = String(120)
    columns = [_col("id", Integer(), nullable=False), _col("email", email_type, nullable=email_nullable)]
    columns.extend(extra_columns)
    inspector.add_table(
        "public",
        "users",
        columns=columns,
        pk={"name": None, "constrained_columns": ["id"]},
        uniques=[{"name": "uq_users_email", "column_names": ["email"]}],
    )


# ==================================================================================================
# SchemaDiff container
# ==================================================================================================


def test_schema_diff_add():
    diff = SchemaDiff()
    diff.add(CreateSchemaOp(schema="ana"))
    assert [op.schema for op in _ops(diff, CreateSchemaOp)] == ["ana"]


def test_diff_no_work_returns_empty_operations():
    metadata = MetaData()
    result = _diff(metadata, schemas=set(), inspector=FakeInspector())
    assert result.operations == []


# ==================================================================================================
# Schemas
# ==================================================================================================


def test_diff_creates_missing_schema():
    metadata = MetaData()
    Table("users", metadata, Column("id", Integer), schema="public")
    inspector = FakeInspector()
    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    assert [op.schema for op in _ops(result, CreateSchemaOp)] == ["public"]


def test_diff_skips_existing_schema_and_ignores_other_schemas():
    metadata = MetaData()
    Table("users", metadata, Column("id", Integer), schema="public")
    Table("audit", metadata, Column("id", Integer), schema="other")
    inspector = FakeInspector(schemas=["public"])
    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    assert _ops(result, CreateSchemaOp) == []
    assert len(_ops(result, CreateTableOp)) == 1
    assert not any(op.table == "audit" for op in _ops(result, CreateTableOp))


# ==================================================================================================
# Tables
# ==================================================================================================


def test_diff_creates_missing_table_with_index_and_unique():
    metadata = MetaData()
    Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("email", String(120)),
        Index("ix_users_email", "email"),
        UniqueConstraint("email", name="uq_users_email"),
        schema="public",
    )
    inspector = FakeInspector(schemas=["public"])
    result = _diff(metadata, schemas={"public"}, inspector=inspector)

    create = _ops(result, CreateTableOp)
    assert len(create) == 1
    assert create[0].table == "users"
    assert [c.name for c in create[0].columns] == ["id", "email"]

    idx = _ops(result, CreateIndexOp)
    assert len(idx) == 1
    assert idx[0].index_name == "ix_users_email"
    assert idx[0].column_names == ("email",)

    uq = _ops(result, AddUniqueConstraintOp)
    assert len(uq) == 1
    assert uq[0].constraint_name == "uq_users_email"
    assert uq[0].columns == ("email",)


def test_diff_drops_extra_table():
    metadata = MetaData()
    Table("users", metadata, Column("id", Integer), schema="public")
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table("public", "users", columns=[_col("id", Integer())])
    inspector.add_table("public", "orphan", columns=[_col("id", Integer())])

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    drops = _ops(result, DropTableOp)
    assert [op.table for op in drops] == ["orphan"]


def test_diff_drops_orphan_views():
    metadata = MetaData()
    Table("users", metadata, Column("id", Integer), schema="public")
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table("public", "users", columns=[_col("id", Integer())])
    inspector.add_view("public", "v_stale")

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    views = _ops(result, DropViewOp)
    assert [op.view for op in views] == ["v_stale"]


def test_diff_detects_table_rename():
    metadata = MetaData()
    Table("channels", metadata, Column("id", Integer, primary_key=True), schema="public")
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "chs",
        columns=[_col("id", Integer(), nullable=False)],
        pk={"name": None, "constrained_columns": ["id"]},
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    renames = _ops(result, RenameTableOp)
    assert len(renames) == 1
    assert renames[0].old_name == "chs"
    assert renames[0].new_name == "channels"
    assert _ops(result, AlterColumnTypeOp) == []


def test_detect_table_renames_matches_similar_names():
    matched = SchemaDiffEngine._detect_table_renames("public", {"zones_hierarchy"}, {"zone_hierarchy"})
    assert matched == [("zone_hierarchy", "zones_hierarchy")]


def test_detect_table_renames_rejects_dissimilar_names():
    assert SchemaDiffEngine._detect_table_renames("public", {"users"}, {"channel_data"}) == []


def test_unscoped_table_raises_for_create():
    metadata = MetaData()
    table = Table("t", metadata, Column("id", Integer))
    inspector = FakeInspector()
    with patch("kronicle.db.migration.engine.schema_diff_engine.inspect", return_value=inspector):
        engine = SchemaDiffEngine(cast(Connection, _connection()), metadata, schemas={"public"})
    with pytest.raises(RuntimeError, match="Unscoped table"):
        engine._build_create_table_op(table)


def test_unscoped_table_raises_for_existing_diff():
    metadata = MetaData()
    table = Table("t", metadata, Column("id", Integer))
    inspector = FakeInspector()
    with patch("kronicle.db.migration.engine.schema_diff_engine.inspect", return_value=inspector):
        engine = SchemaDiffEngine(cast(Connection, _connection()), metadata, schemas={"public"})
    with pytest.raises(RuntimeError, match="Unscoped table"):
        engine._diff_existing_table(SchemaDiff(), table)


# ==================================================================================================
# Columns
# ==================================================================================================


def test_diff_adds_column():
    metadata = MetaData()
    _users_table(metadata, extra_columns=[Column("age", Integer, nullable=True)])
    inspector = FakeInspector(schemas=["public"])
    _users_in_db(inspector)

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    adds = _ops(result, AddColumnOp)
    assert len(adds) == 1
    assert adds[0].table == "users"
    assert adds[0].column_name == "age"
    assert adds[0].column_def.name == "age"


def test_diff_adds_non_nullable_column_without_default():
    metadata = MetaData()
    _users_table(metadata, extra_columns=[Column("age", Integer, nullable=False)])
    inspector = FakeInspector(schemas=["public"])
    _users_in_db(inspector)

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    assert _ops(result, AddColumnOp) == []
    non_null = _ops(result, AddNonNullableColumnOp)
    assert len(non_null) == 1
    assert non_null[0].column_name == "age"


def test_diff_adds_non_nullable_column_with_default_as_plain_add():
    metadata = MetaData()
    _users_table(metadata, extra_columns=[Column("age", Integer, nullable=False, server_default="0")])
    inspector = FakeInspector(schemas=["public"])
    _users_in_db(inspector)

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    assert _ops(result, AddNonNullableColumnOp) == []
    assert len(_ops(result, AddColumnOp)) == 1


def test_diff_drops_column():
    metadata = MetaData()
    _users_table(metadata)
    inspector = FakeInspector(schemas=["public"])
    _users_in_db(inspector, extra_columns=[_col("legacy", String(30), nullable=True)])

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    drops = _ops(result, DropColumnOp)
    assert [op.column_name for op in drops] == ["legacy"]


def test_diff_detects_column_rename():
    metadata = MetaData()
    Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("description", String(120)),
        schema="public",
    )
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "users",
        columns=[_col("id", Integer(), nullable=False), _col("decription", String(120), nullable=True)],
        pk={"name": None, "constrained_columns": ["id"]},
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    renames = _ops(result, RenameColumnOp)
    assert len(renames) == 1
    assert renames[0].old_name == "decription"
    assert renames[0].new_name == "description"


def test_diff_column_type_mismatch():
    metadata = MetaData()
    _users_table(metadata)
    inspector = FakeInspector(schemas=["public"])
    _users_in_db(inspector)

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    assert _ops(result, AlterColumnTypeOp) == []

    inspector._columns[("public", "users")][1]["type"] = Integer()
    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    alters = _ops(result, AlterColumnTypeOp)
    assert len(alters) == 1
    assert alters[0].column == "email"
    assert alters[0].new_type.compile(dialect=_connection().dialect) == "VARCHAR(120)"


def test_diff_column_nullability_mismatch():
    metadata = MetaData()
    users = _users_table(metadata)
    users.c.email.nullable = False
    inspector = FakeInspector(schemas=["public"])
    _users_in_db(inspector, email_nullable=True)

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    alters = _ops(result, AlterColumnNullabilityOp)
    assert len(alters) == 1
    assert alters[0].column == "email"
    assert alters[0].nullable is False


# ==================================================================================================
# Primary keys
# ==================================================================================================


def test_diff_adds_primary_key():
    metadata = MetaData()
    Table("users", metadata, Column("id", Integer, primary_key=True), schema="public")
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "users",
        columns=[_col("id", Integer(), nullable=True)],
        pk={"name": None, "constrained_columns": []},
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    adds = _ops(result, AddPrimaryKeyOp)
    assert len(adds) == 1
    assert adds[0].constraint_name == "users_pkey"
    assert adds[0].columns == ("id",)


def test_diff_drops_primary_key():
    metadata = MetaData()
    Table("users", metadata, Column("id", Integer), schema="public")
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "users",
        columns=[_col("id", Integer(), nullable=False)],
        pk={"name": "users_pkey", "constrained_columns": ["id"]},
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    drops = _ops(result, DropPrimaryKeyOp)
    assert len(drops) == 1
    assert drops[0].constraint_name == "users_pkey"


def test_diff_drops_and_recreates_primary_key():
    metadata = MetaData()
    Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("ts", Integer, primary_key=True),
        schema="public",
    )
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "users",
        columns=[_col("id", Integer(), nullable=False), _col("ts", Integer(), nullable=False)],
        pk={"name": None, "constrained_columns": ["id"]},
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    drops = _ops(result, DropPrimaryKeyOp)
    adds = _ops(result, AddPrimaryKeyOp)
    assert len(drops) == 1 and len(adds) == 1
    assert [op.constraint_name for op in adds] == ["users_pkey"]
    assert adds[0].columns == ("id", "ts")


# ==================================================================================================
# Indexes
# ==================================================================================================


def test_diff_adds_index():
    metadata = MetaData()
    Table("users", metadata, Column("email", String(120)), Index("ix_users_email", "email"), schema="public")
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table("public", "users", columns=[_col("email", String(120))])

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    adds = _ops(result, CreateIndexOp)
    assert len(adds) == 1
    assert adds[0].index_name == "ix_users_email"
    assert adds[0].column_names == ("email",)
    assert adds[0].unique is False


def test_diff_drops_index():
    metadata = MetaData()
    Table("users", metadata, Column("email", String(120)), schema="public")
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "users",
        columns=[_col("email", String(120))],
        indexes=[{"name": "ix_users_email_old", "column_names": ["email"], "unique": False, "dialect_options": {}}],
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    drops = _ops(result, DropIndexOp)
    assert [op.index_name for op in drops] == ["ix_users_email_old"]


def test_diff_skips_auto_pkey_indexes():
    metadata = MetaData()
    Table("users", metadata, Column("id", Integer), schema="public")
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "users",
        columns=[_col("id", Integer(), nullable=False)],
        indexes=[{"name": "users_pkey", "column_names": ["id"], "unique": True, "dialect_options": {}}],
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    assert _ops(result, DropIndexOp) == []


def test_diff_skips_indexes_backing_constraints():
    metadata = MetaData()
    Table("users", metadata, Column("email", String(120)), schema="public")
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "users",
        columns=[_col("email", String(120))],
        indexes=[{"name": "uq_users_email", "column_names": ["email"], "unique": True, "duplicates_constraint": True}],
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    assert _ops(result, DropIndexOp) == []


# ==================================================================================================
# Constraints
# ==================================================================================================


def test_diff_adds_unique_constraint():
    metadata = MetaData()
    _users_table(metadata, with_email_constraint=True)
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "users",
        columns=[_col("id", Integer(), nullable=False), _col("email", String(120))],
        pk={"name": None, "constrained_columns": ["id"]},
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    adds = _ops(result, AddUniqueConstraintOp)
    assert len(adds) == 1
    assert adds[0].constraint_name == "uq_users_email"
    assert adds[0].columns == ("email",)


def test_diff_drops_unique_constraint():
    metadata = MetaData()
    _users_table(metadata, with_email_constraint=False)
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "users",
        columns=[_col("id", Integer(), nullable=False), _col("email", String(120))],
        pk={"name": None, "constrained_columns": ["id"]},
        uniques=[{"name": "uq_users_email", "column_names": ["email"]}],
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    drops = _ops(result, DropConstraintOp)
    assert [op.constraint_name for op in drops] == ["uq_users_email"]


def test_diff_ignores_standalone_unique_index_that_shadows_uq_constraint():
    """A unique INDEX (not a pg_constraint) must not yield a spurious add/drop."""
    metadata = MetaData()
    _users_table(metadata, with_email_constraint=True)  # metadata has uq_users_email
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "users",
        columns=[_col("id", Integer(), nullable=False), _col("email", String(120))],
        pk={"name": None, "constrained_columns": ["id"]},
        # DB exposes it ONLY as a standalone unique index (no constraint row)
        indexes=[
            {
                "name": "uq_users_email",
                "unique": True,
                "column_names": ["email"],
                "duplicates_constraint": None,
            }
        ],
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    assert _ops(result, AddUniqueConstraintOp) == []
    assert _ops(result, DropConstraintOp) == []
    assert _ops(result, DropIndexOp) == []


def test_diff_renames_unique_constraint():
    metadata = MetaData()
    Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("email", String(120)),
        UniqueConstraint("email", name="uq_users_email_new"),
        schema="public",
    )
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "users",
        columns=[_col("id", Integer(), nullable=False), _col("email", String(120))],
        pk={"name": None, "constrained_columns": ["id"]},
        uniques=[{"name": "uq_users_email", "column_names": ["email"]}],
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    renames = _ops(result, RenameConstraintOp)
    assert len(renames) == 1
    assert renames[0].old_name == "uq_users_email"
    assert renames[0].new_name == "uq_users_email_new"
    assert _ops(result, AddUniqueConstraintOp) == []
    assert _ops(result, DropConstraintOp) == []


def test_diff_adds_check_constraint():
    metadata = MetaData()
    Table(
        "users",
        metadata,
        Column("id", Integer),
        CheckConstraint("id >= 0", name="ck_users_id"),
        schema="public",
    )
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table("public", "users", columns=[_col("id", Integer())])

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    adds = _ops(result, AddCheckConstraintOp)
    assert len(adds) == 1
    assert adds[0].constraint_name == "ck_users_id"
    assert "id >= 0" in adds[0].sqltext


def test_diff_drops_check_constraint():
    metadata = MetaData()
    Table("users", metadata, Column("id", Integer), schema="public")
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "users",
        columns=[_col("id", Integer())],
        checks=[{"name": "ck_users_id", "sqltext": "id >= 0"}],
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    drops = _ops(result, DropConstraintOp)
    assert [op.constraint_name for op in drops] == ["ck_users_id"]


def test_diff_dedups_drop_index_backed_by_dropped_constraint():
    metadata = MetaData()
    _users_table(metadata, with_email_constraint=False)
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "users",
        columns=[_col("id", Integer(), nullable=False), _col("email", String(120))],
        pk={"name": None, "constrained_columns": ["id"]},
        uniques=[{"name": "uq_users_email", "column_names": ["email"]}],
        indexes=[{"name": "uq_users_email", "column_names": ["email"], "unique": True, "dialect_options": {}}],
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    drops = _ops(result, DropConstraintOp)
    assert [op.constraint_name for op in drops] == ["uq_users_email"]
    assert _ops(result, DropIndexOp) == []


# ==================================================================================================
# Foreign keys
# ==================================================================================================


def test_diff_adds_foreign_key():
    metadata = MetaData()
    Table("channels", metadata, Column("id", Integer, primary_key=True), schema="public")
    Table(
        "messages",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("channel_id", Integer, ForeignKey("public.channels.id", name="fk_messages_channel_id")),
        schema="public",
    )
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "channels",
        columns=[_col("id", Integer(), nullable=False)],
        pk={"name": None, "constrained_columns": ["id"]},
    )
    inspector.add_table(
        "public",
        "messages",
        columns=[_col("id", Integer(), nullable=False), _col("channel_id", Integer(), nullable=True)],
        pk={"name": None, "constrained_columns": ["id"]},
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    adds = _ops(result, AddForeignKeyOp)
    assert len(adds) == 1
    assert adds[0].table == "messages"
    assert adds[0].constraint_name == "fk_messages_channel_id"
    assert adds[0].referred_table == "channels"
    assert adds[0].local_columns == ("channel_id",)
    assert adds[0].referred_columns == ("id",)


def test_diff_drops_foreign_key():
    metadata = MetaData()
    Table("channels", metadata, Column("id", Integer, primary_key=True), schema="public")
    Table(
        "messages",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("channel_id", Integer),
        schema="public",
    )
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "channels",
        columns=[_col("id", Integer(), nullable=False)],
        pk={"name": None, "constrained_columns": ["id"]},
    )
    inspector.add_table(
        "public",
        "messages",
        columns=[_col("id", Integer(), nullable=False), _col("channel_id", Integer(), nullable=True)],
        pk={"name": None, "constrained_columns": ["id"]},
        fks=[
            {
                "name": "fk_messages_channel_id",
                "constrained_columns": ["channel_id"],
                "referred_table": "channels",
                "referred_columns": ["id"],
            }
        ],
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    drops = _ops(result, DropForeignKeyOp)
    assert len(drops) == 1
    assert drops[0].constraint_name == "fk_messages_channel_id"


def test_diff_changes_foreign_key_ondelete():
    metadata = MetaData()
    Table("channels", metadata, Column("id", Integer, primary_key=True), schema="public")
    Table(
        "messages",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("channel_id", Integer, ForeignKey("public.channels.id", ondelete="CASCADE")),
        schema="public",
    )
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "channels",
        columns=[_col("id", Integer(), nullable=False)],
        pk={"name": None, "constrained_columns": ["id"]},
    )
    inspector.add_table(
        "public",
        "messages",
        columns=[_col("id", Integer(), nullable=False), _col("channel_id", Integer(), nullable=True)],
        pk={"name": None, "constrained_columns": ["id"]},
        fks=[
            {
                "name": None,
                "constrained_columns": ["channel_id"],
                "referred_table": "channels",
                "referred_columns": ["id"],
                "options": {"ondelete": "SET NULL", "onupdate": None},
            }
        ],
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    adds = _ops(result, AddForeignKeyOp)
    assert len(adds) == 1
    assert adds[0].constraint_name == "messages_channel_id_fkey"
    assert adds[0].ondelete == "CASCADE"
    assert _ops(result, DropForeignKeyOp) == []


def test_diff_recreates_renamed_foreign_key():
    # DB FK keys are column-based (_fk_db_key always returns ("cols", ...)),
    # so a renamed FK surfaces as drop-old + add-new rather than a rename op.
    metadata = MetaData()
    Table("channels", metadata, Column("id", Integer, primary_key=True), schema="public")
    Table(
        "messages",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("channel_id", Integer, ForeignKey("public.channels.id", name="fk_msg_ch_id")),
        schema="public",
    )
    inspector = FakeInspector(schemas=["public"])
    inspector.add_table(
        "public",
        "channels",
        columns=[_col("id", Integer(), nullable=False)],
        pk={"name": None, "constrained_columns": ["id"]},
    )
    inspector.add_table(
        "public",
        "messages",
        columns=[_col("id", Integer(), nullable=False), _col("channel_id", Integer(), nullable=True)],
        pk={"name": None, "constrained_columns": ["id"]},
        fks=[
            {
                "name": "fk_messages_channel_id",
                "constrained_columns": ["channel_id"],
                "referred_table": "channels",
                "referred_columns": ["id"],
            }
        ],
    )

    result = _diff(metadata, schemas={"public"}, inspector=inspector)
    drops = _ops(result, DropForeignKeyOp)
    adds = _ops(result, AddForeignKeyOp)
    assert len(drops) == 1
    assert drops[0].constraint_name == "fk_messages_channel_id"
    assert len(adds) == 1
    assert adds[0].constraint_name == "fk_msg_ch_id"
    assert _ops(result, RenameConstraintOp) == []


def test_fk_meta_key_named_and_unnamed():
    metadata = MetaData()
    Table("channels", metadata, Column("id", Integer, primary_key=True), schema="public")
    messages = Table("messages", metadata, Column("channel_id", Integer), schema="public")

    named = ForeignKeyConstraint(["channel_id"], ["public.channels.id"], name="fk_msg")
    messages.append_constraint(named)
    assert SchemaDiffEngine._fk_meta_key(named) == ("name", "fk_msg")

    unnamed = ForeignKeyConstraint(["channel_id"], ["public.channels.id"])
    messages.append_constraint(unnamed)
    assert SchemaDiffEngine._fk_meta_key(unnamed) == ("cols", ("channel_id",), "channels", ("id",))


def test_fk_db_key_uses_columns():
    fk_def = cast(
        ReflectedForeignKeyConstraint,
        {"constrained_columns": ["channel_id"], "referred_table": "channels", "referred_columns": ["id"]},
    )
    assert SchemaDiffEngine._fk_db_key(fk_def) == ("cols", ("channel_id",), "channels", ("id",))
