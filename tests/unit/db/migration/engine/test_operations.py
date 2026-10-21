# tests/unit/db/migration/engine/test_operations.py
"""Unit tests for kronicle.db.migration.engine.operations.

The operations are immutable intents; ``apply()`` delegates to a (mocked)
Alembic ``Operations`` context, so every test here asserts the exact delegation.
"""

from unittest.mock import MagicMock

import pytest
from sqlalchemy import Column, Integer, String

from kronicle.db.migration.engine.operations import (
    AddCheckConstraintOp,
    AddColumnOp,
    AddExcludeConstraintOp,
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
    DestructivePolicy,
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
    SafePolicy,
    SafetyLevel,
    WarningPolicy,
    _schema,
)

# ==================================================================================================
# Safety model
# ==================================================================================================


def test_requires_confirmation_only_for_destructive():
    assert SafePolicy().requires_confirmation() is False
    assert WarningPolicy().requires_confirmation() is False
    assert DestructivePolicy().requires_confirmation() is True


def test_policy_levels():
    assert SafePolicy.level == SafetyLevel.SAFE
    assert WarningPolicy.level == SafetyLevel.WARNING
    assert DestructivePolicy.level == SafetyLevel.DESTRUCTIVE


# ==================================================================================================
# Identity (describe / op_id)
# ==================================================================================================

_DESCRIBE_CASES = [
    (CreateSchemaOp(schema="core"), "create_schema:core"),
    (CreateTableOp(schema="core", table="users"), "create_table:core.users"),
    (
        AddColumnOp(schema="core", table="users", column_name="email", column_def=Column("email", String)),
        "add_column:core.users.email",
    ),
    (
        AddNonNullableColumnOp(schema="core", table="users", column_name="email", column_def=Column("email", String)),
        "add_nonnullable_column:core.users.email",
    ),
    (
        CreateIndexOp(schema="core", table="users", index_name="ix_email", column_names=("email",)),
        "create_index:core.users.ix_email",
    ),
    (
        AddUniqueConstraintOp(schema="core", table="users", constraint_name="uq_email", columns=("email",)),
        "add_unique:core.users.uq_email",
    ),
    (
        AddCheckConstraintOp(schema="core", table="users", constraint_name="ck_age", sqltext="age > 0"),
        "add_check:core.users.ck_age",
    ),
    (
        AddExcludeConstraintOp(schema="core", table="sensors", constraint_name="ex_zone", elements=()),
        "add_exclude:core.sensors.ex_zone",
    ),
    (RenameTableOp(schema="core", old_name="user", new_name="users"), "rename_table:core.user->users"),
    (
        RenameColumnOp(schema="core", table="users", old_name="mail", new_name="email"),
        "rename_column:core.users.mail->email",
    ),
    (
        RenameConstraintOp(schema="core", table="users", old_name="ck_old", new_name="ck_new"),
        "rename_constraint:core.users.ck_old->ck_new",
    ),
    (
        AddForeignKeyOp(
            schema="core",
            table="messages",
            constraint_name="fk_ch",
            referred_table="channels",
            local_columns=("channel_id",),
            referred_columns=("id",),
        ),
        "add_foreignkey:core.messages.fk_ch",
    ),
    (
        AlterColumnTypeOp(schema="core", table="users", column="email", old_type=Integer(), new_type=String()),
        "alter_type:core.users.email",
    ),
    (
        AlterColumnNullabilityOp(schema="core", table="users", column="email", nullable=False),
        "alter_nullable:core.users.email",
    ),
    (
        DropPrimaryKeyOp(schema="core", table="users", constraint_name="users_pkey"),
        "drop_primary_key:core.users.users_pkey",
    ),
    (
        AddPrimaryKeyOp(schema="core", table="users", constraint_name="users_pkey", columns=("id",)),
        "add_primary_key:core.users.users_pkey",
    ),
    (DropConstraintOp(schema="core", table="users", constraint_name="uq_email"), "drop_constraint:core.users.uq_email"),
    (DropForeignKeyOp(schema="core", table="messages", constraint_name="fk_ch"), "drop_foreignkey:core.messages.fk_ch"),
    (DropColumnOp(schema="core", table="users", column_name="email"), "drop_column:core.users.email"),
    (DropIndexOp(schema="core", table="users", index_name="ix_email"), "drop_index:core.users.ix_email"),
    (DropTableOp(schema="core", table="users"), "drop_table:core.users"),
    (DropViewOp(schema="core", view="v_stale"), "drop_view:core.v_stale"),
]


@pytest.mark.parametrize("op,expected", _DESCRIBE_CASES)
def test_describe_and_op_id(op, expected):
    assert op.describe() == expected
    assert op.op_id == expected


# ==================================================================================================
# Dependency handling
# ==================================================================================================


def test_with_dependency_returns_copy_with_extended_depends_on():
    base = CreateSchemaOp(schema="core")
    other = CreateTableOp(schema="core", table="users", columns=())
    derived = base.with_dependency(other)

    assert derived is not base
    assert derived.depends_on == frozenset({"create_table:core.users"})
    assert base.depends_on == frozenset()


# ==================================================================================================
# Abstract guards
# ==================================================================================================


class _ConcreteGuard(DbStructureOperation):
    """Concrete subclass whose methods only delegate to the abstract originals."""

    priority = 1
    safety = SafePolicy()

    def describe(self):
        return super().describe()  # type: ignore[abstract]  # deliberate: exercise the ABC guard

    def apply(self, op):
        return super().apply(op)  # type: ignore[abstract]  # deliberate: exercise the ABC guard


def test_abstract_describe_raises_not_implemented():
    with pytest.raises(NotImplementedError):
        _ConcreteGuard().describe()


def test_abstract_apply_raises_not_implemented():
    with pytest.raises(NotImplementedError):
        _ConcreteGuard().apply(MagicMock())


# ==================================================================================================
# Helpers
# ==================================================================================================


def test_schema_wrapper_delegates():
    sentinel = object()
    seen = []

    def fn(op):
        seen.append(op)
        return sentinel

    arg = MagicMock()
    assert _schema(fn)(arg) is sentinel
    assert seen == [arg]


# ==================================================================================================
# apply() delegations
# ==================================================================================================


def test_apply_create_schema_executes_sql():
    alembic = MagicMock()
    CreateSchemaOp(schema="core").apply(alembic)
    alembic.execute.assert_called_once_with("CREATE SCHEMA IF NOT EXISTS core")


def test_apply_create_table():
    alembic = MagicMock()
    columns = (Column("id", Integer),)
    CreateTableOp(schema="core", table="users", columns=columns).apply(alembic)
    alembic.create_table.assert_called_once_with("users", *columns, schema="core")


def test_apply_add_column():
    alembic = MagicMock()
    column_def = Column("email", String)
    AddColumnOp(schema="core", table="users", column_name="email", column_def=column_def).apply(alembic)
    alembic.add_column.assert_called_once_with("users", column_def, schema="core")


def test_apply_add_non_nullable_column():
    alembic = MagicMock()
    column_def = Column("email", String, nullable=False)
    AddNonNullableColumnOp(schema="core", table="users", column_name="email", column_def=column_def).apply(alembic)
    alembic.add_column.assert_called_once_with("users", column_def, schema="core")


def test_apply_create_index():
    alembic = MagicMock()
    CreateIndexOp(schema="core", table="users", index_name="ix_email", column_names=("email",), unique=True).apply(
        alembic
    )
    alembic.create_index.assert_called_once_with("ix_email", "users", ("email",), schema="core", unique=True)


def test_apply_add_unique_constraint():
    alembic = MagicMock()
    AddUniqueConstraintOp(schema="core", table="users", constraint_name="uq_email", columns=("email",)).apply(alembic)
    alembic.create_unique_constraint.assert_called_once_with(
        constraint_name="uq_email", table_name="users", columns=("email",), schema="core"
    )


def test_apply_add_check_constraint():
    alembic = MagicMock()
    AddCheckConstraintOp(schema="core", table="users", constraint_name="ck_age", sqltext="age > 0").apply(alembic)
    alembic.create_check_constraint.assert_called_once_with("ck_age", "users", "age > 0", schema="core")


def test_apply_add_exclude_constraint():
    alembic = MagicMock()
    elements = (("zone", "="),)
    AddExcludeConstraintOp(schema="core", table="sensors", constraint_name="ex_zone", elements=elements).apply(alembic)
    alembic.create_exclude_constraint.assert_called_once_with("ex_zone", "sensors", elements, schema="core")


def test_apply_rename_table():
    alembic = MagicMock()
    RenameTableOp(schema="core", old_name="user", new_name="users").apply(alembic)
    alembic.rename_table.assert_called_once_with("user", "users", schema="core")


def test_apply_rename_column():
    alembic = MagicMock()
    RenameColumnOp(schema="core", table="users", old_name="mail", new_name="email").apply(alembic)
    alembic.alter_column.assert_called_once_with("users", "mail", new_column_name="email", schema="core")


def test_apply_rename_constraint_executes_sql():
    alembic = MagicMock()
    RenameConstraintOp(schema="core", table="users", old_name="ck_old", new_name="ck_new").apply(alembic)
    alembic.execute.assert_called_once_with("ALTER TABLE core.users RENAME CONSTRAINT ck_old TO ck_new")


def test_apply_add_foreign_key_defaults_referent_schema_to_source():
    alembic = MagicMock()
    AddForeignKeyOp(
        schema="core",
        table="messages",
        constraint_name="fk_ch",
        referred_table="channels",
        local_columns=("channel_id",),
        referred_columns=("id",),
    ).apply(alembic)
    alembic.create_foreign_key.assert_called_once_with(
        "fk_ch",
        "messages",
        "channels",
        ["channel_id"],
        ["id"],
        source_schema="core",
        referent_schema="core",
        ondelete=None,
        onupdate=None,
    )


def test_apply_add_foreign_key_with_explicit_referent_schema():
    alembic = MagicMock()
    AddForeignKeyOp(
        schema="core",
        table="messages",
        constraint_name="fk_ch",
        referred_table="channels",
        local_columns=("channel_id",),
        referred_columns=("id",),
        referred_schema="rbac",
        ondelete="CASCADE",
        onupdate="SET NULL",
    ).apply(alembic)
    _, kwargs = alembic.create_foreign_key.call_args
    assert kwargs["referent_schema"] == "rbac"
    assert kwargs["ondelete"] == "CASCADE"
    assert kwargs["onupdate"] == "SET NULL"


def test_apply_alter_column_type():
    alembic = MagicMock()
    new_type = String(255)
    AlterColumnTypeOp(schema="core", table="users", column="email", old_type=Integer(), new_type=new_type).apply(
        alembic
    )
    alembic.alter_column.assert_called_once_with("users", "email", type_=new_type, schema="core")


def test_apply_alter_column_nullability():
    alembic = MagicMock()
    AlterColumnNullabilityOp(schema="core", table="users", column="email", nullable=False).apply(alembic)
    alembic.alter_column.assert_called_once_with("users", "email", nullable=False, schema="core")


def test_apply_drop_primary_key():
    alembic = MagicMock()
    DropPrimaryKeyOp(schema="core", table="users", constraint_name="users_pkey").apply(alembic)
    alembic.drop_constraint.assert_called_once_with("users_pkey", "users", type_="primary", schema="core")


def test_apply_add_primary_key():
    alembic = MagicMock()
    AddPrimaryKeyOp(schema="core", table="users", constraint_name="users_pkey", columns=("id",)).apply(alembic)
    alembic.create_primary_key.assert_called_once_with("users_pkey", "users", ["id"], schema="core")


def test_apply_drop_constraint():
    alembic = MagicMock()
    DropConstraintOp(schema="core", table="users", constraint_name="uq_email").apply(alembic)
    alembic.drop_constraint.assert_called_once_with("uq_email", "users", schema="core")


def test_apply_drop_foreign_key():
    alembic = MagicMock()
    DropForeignKeyOp(schema="core", table="messages", constraint_name="fk_ch").apply(alembic)
    alembic.drop_constraint.assert_called_once_with("fk_ch", "messages", type_="foreignkey", schema="core")


def test_apply_drop_column():
    alembic = MagicMock()
    DropColumnOp(schema="core", table="users", column_name="email").apply(alembic)
    alembic.drop_column.assert_called_once_with("users", "email", schema="core")


def test_apply_drop_index():
    alembic = MagicMock()
    DropIndexOp(schema="core", table="users", index_name="ix_email").apply(alembic)
    alembic.drop_index.assert_called_once_with("ix_email", table_name="users", schema="core")


def test_apply_drop_table():
    alembic = MagicMock()
    DropTableOp(schema="core", table="users").apply(alembic)
    alembic.drop_table.assert_called_once_with("users", schema="core")


def test_apply_drop_view_executes_sql():
    alembic = MagicMock()
    DropViewOp(schema="core", view="v_stale").apply(alembic)
    alembic.execute.assert_called_once_with("DROP VIEW IF EXISTS core.v_stale")
