# kronicle/db/migration/operations.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from typing import ClassVar, FrozenSet

from alembic.operations import Operations
from sqlalchemy import Column
from sqlalchemy.sql import quoted_name
from sqlalchemy.types import TypeEngine

# ==================================================================================================
# Safety model
# ==================================================================================================


class SafetyLevel:
    SAFE = "safe"
    WARNING = "warning"
    DESTRUCTIVE = "destructive"


@dataclass(frozen=True, kw_only=True)
class SafetyPolicy(ABC):
    level: ClassVar[str]

    def requires_confirmation(self) -> bool:
        return self.level == SafetyLevel.DESTRUCTIVE


class SafePolicy(SafetyPolicy):
    level = SafetyLevel.SAFE


class WarningPolicy(SafetyPolicy):
    level = SafetyLevel.WARNING


class DestructivePolicy(SafetyPolicy):
    level = SafetyLevel.DESTRUCTIVE

    def requires_confirmation(self) -> bool:
        return True


# ==================================================================================================
# Base operation
# ==================================================================================================


@dataclass(frozen=True, kw_only=True)
class DbStructureOperation(ABC):
    """
    Immutable migration intent.

    Design goals:
    - deterministic diff representation
    - dependency-safe (DAG friendly)
    - directly executable via Alembic
    """

    # dependency graph (by op_id, not object references)
    depends_on: FrozenSet[str] = field(default_factory=frozenset, repr=False)
    priority: ClassVar[int]
    safety: ClassVar[SafetyPolicy]

    # ----------------------------------------------------------------------------------------------
    # Identity
    # ----------------------------------------------------------------------------------------------
    @property
    def op_id(self) -> str:
        return self.describe()

    # ----------------------------------------------------------------------------------------------
    # Dependency handling
    # ----------------------------------------------------------------------------------------------
    def with_dependency(self, other: DbStructureOperation) -> DbStructureOperation:
        return replace(
            self,
            depends_on=self.depends_on | {other.op_id},
        )

    # ----------------------------------------------------------------------------------------------
    # Required API
    # ----------------------------------------------------------------------------------------------
    @abstractmethod
    def describe(self) -> str:
        """Human + machine readable identifier."""
        raise NotImplementedError

    @abstractmethod
    def apply(self, op: Operations) -> None:
        """Execute operation using Alembic Operations context."""
        raise NotImplementedError


# ==================================================================================================
# Helpers
# ==================================================================================================


def _schema(op_fn):
    """
    Optional wrapper for consistent future logging/tracing.
    """

    def wrapper(op: Operations):
        return op_fn(op)

    return wrapper


# ==================================================================================================
# CREATE
# ==================================================================================================


@dataclass(frozen=True)
class CreateSchemaOp(DbStructureOperation):
    schema: str = ""

    priority = 5
    safety = SafePolicy()

    def describe(self):
        return f"create_schema:{self.schema}"

    def apply(self, op: Operations) -> None:
        op.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")


@dataclass(frozen=True)
class CreateTableOp(DbStructureOperation):
    schema: str
    table: str
    columns: tuple = field(default_factory=tuple)

    priority = 10
    safety = SafePolicy()

    def describe(self):
        return f"create_table:{self.schema}.{self.table}"

    def apply(self, op: Operations) -> None:
        op.create_table(
            self.table,
            *self.columns,
            schema=self.schema,
        )


@dataclass(frozen=True)
class AddColumnOp(DbStructureOperation):
    schema: str
    table: str
    column_name: str
    column_def: Column  # SQLAlchemy Column

    priority = 20
    safety = SafePolicy()

    def describe(self):
        return f"add_column:{self.schema}.{self.table}.{self.column_name}"

    def apply(self, op: Operations) -> None:
        op.add_column(
            self.table,
            self.column_def,
            schema=self.schema,
        )


@dataclass(frozen=True)
class AddNonNullableColumnOp(DbStructureOperation):
    schema: str
    table: str
    column_name: str
    column_def: Column  # SQLAlchemy Column

    priority = 20
    safety = WarningPolicy()

    def describe(self):
        return f"add_nonnullable_column:{self.schema}.{self.table}.{self.column_name}"

    def apply(self, op: Operations) -> None:
        op.add_column(
            self.table,
            self.column_def,
            schema=self.schema,
        )


@dataclass(frozen=True)
class CreateIndexOp(DbStructureOperation):
    schema: str
    table: str
    index_name: str
    column_names: tuple[str, ...]
    unique: bool = False

    priority = 25
    safety = SafePolicy()

    def describe(self):
        return f"create_index:{self.schema}.{self.table}.{self.index_name}"

    def apply(self, op: Operations) -> None:
        op.create_index(
            self.index_name,
            self.table,
            self.column_names,
            schema=self.schema,
            unique=self.unique,
        )


@dataclass(frozen=True)
class AddUniqueConstraintOp(DbStructureOperation):
    schema: str
    table: str
    constraint_name: str
    columns: tuple

    priority = 30
    safety = WarningPolicy()

    def describe(self):
        return f"add_unique:{self.schema}.{self.table}.{self.constraint_name}"

    def apply(self, op: Operations) -> None:
        op.create_unique_constraint(
            constraint_name=self.constraint_name,
            table_name=self.table,
            columns=self.columns,
            schema=self.schema,
        )


@dataclass(frozen=True)
class AddCheckConstraintOp(DbStructureOperation):
    schema: str
    table: str
    constraint_name: str
    sqltext: str

    priority = 35
    safety = WarningPolicy()

    def describe(self):
        return f"add_check:{self.schema}.{self.table}.{self.constraint_name}"

    def apply(self, op: Operations) -> None:
        op.create_check_constraint(
            self.constraint_name,
            self.table,
            self.sqltext,
            schema=self.schema,
        )


@dataclass(frozen=True)
class AddExcludeConstraintOp(DbStructureOperation):
    schema: str
    table: str
    constraint_name: str
    elements: tuple

    priority = 36
    safety = WarningPolicy()

    def describe(self):
        return f"add_exclude:{self.schema}.{self.table}.{self.constraint_name}"

    def apply(self, op: Operations) -> None:
        op.create_exclude_constraint(
            self.constraint_name,
            self.table,
            self.elements,
            schema=self.schema,
        )


# ==================================================================================================
# RENAME
# ==================================================================================================


@dataclass(frozen=True)
class RenameTableOp(DbStructureOperation):
    schema: str
    old_name: str
    new_name: str

    priority = 15
    safety = WarningPolicy()

    def describe(self):
        return f"rename_table:{self.schema}.{self.old_name}->{self.new_name}"

    def apply(self, op: Operations) -> None:
        op.rename_table(
            self.old_name,
            self.new_name,
            schema=self.schema,
        )


@dataclass(frozen=True)
class RenameColumnOp(DbStructureOperation):
    schema: str
    table: str
    old_name: str
    new_name: str

    priority = 50
    safety = WarningPolicy()

    def describe(self):
        return f"rename_column:{self.schema}.{self.table}.{self.old_name}->{self.new_name}"

    def apply(self, op: Operations) -> None:
        op.alter_column(
            self.table,
            self.old_name,
            new_column_name=self.new_name,
            schema=self.schema,
        )


@dataclass(frozen=True)
class RenameConstraintOp(DbStructureOperation):
    schema: str
    table: str
    old_name: str
    new_name: str

    priority = 42
    safety = WarningPolicy()

    def describe(self):
        return f"rename_constraint:{self.schema}.{self.table}.{self.old_name}->{self.new_name}"

    def apply(self, op: Operations) -> None:
        op.execute(
            f"ALTER TABLE {self.schema}.{quoted_name(self.table, None)} "
            f"RENAME CONSTRAINT {self.old_name} TO {self.new_name}"
        )


@dataclass(frozen=True)
class AddForeignKeyOp(DbStructureOperation):
    schema: str
    table: str
    constraint_name: str
    referred_table: str
    local_columns: tuple[str, ...]
    referred_columns: tuple[str, ...]
    referred_schema: str | None = None
    ondelete: str | None = None
    onupdate: str | None = None

    priority = 47
    safety = WarningPolicy()

    def describe(self):
        return f"add_foreignkey:{self.schema}.{self.table}.{self.constraint_name}"

    def apply(self, op: Operations) -> None:
        op.create_foreign_key(
            self.constraint_name,
            self.table,
            self.referred_table,
            list(self.local_columns),
            list(self.referred_columns),
            source_schema=self.schema,
            referent_schema=self.referred_schema or self.schema,
            ondelete=self.ondelete,
            onupdate=self.onupdate,
        )


# ==================================================================================================
# ALTER
# ==================================================================================================


@dataclass(frozen=True)
class AlterColumnTypeOp(DbStructureOperation):
    schema: str
    table: str
    column: str
    old_type: TypeEngine
    new_type: TypeEngine

    priority = 60
    safety = WarningPolicy()

    def describe(self):
        return f"alter_type:{self.schema}.{self.table}.{self.column}"

    def apply(self, op: Operations) -> None:
        op.alter_column(
            self.table,
            self.column,
            type_=self.new_type,
            schema=self.schema,
        )


@dataclass(frozen=True)
class AlterColumnNullabilityOp(DbStructureOperation):
    schema: str
    table: str
    column: str
    nullable: bool

    priority = 55
    safety = WarningPolicy()

    def describe(self):
        return f"alter_nullable:{self.schema}.{self.table}.{self.column}"

    def apply(self, op: Operations) -> None:
        op.alter_column(
            self.table,
            self.column,
            nullable=self.nullable,
            schema=self.schema,
        )


# ==================================================================================================
# PRIMARY KEY
# ==================================================================================================


@dataclass(frozen=True)
class DropPrimaryKeyOp(DbStructureOperation):
    schema: str
    table: str
    constraint_name: str

    # Must run before AlterColumnNullabilityOp (55) and DropColumnOp (80),
    # because a PK column cannot be made nullable while still part of the PK.
    priority = 50
    safety = DestructivePolicy()

    def describe(self):
        return f"drop_primary_key:{self.schema}.{self.table}.{self.constraint_name}"

    def apply(self, op: Operations) -> None:
        op.drop_constraint(
            self.constraint_name,
            self.table,
            type_="primary",
            schema=self.schema,
        )


@dataclass(frozen=True)
class AddPrimaryKeyOp(DbStructureOperation):
    schema: str
    table: str
    constraint_name: str
    columns: tuple[str, ...]

    priority = 81
    safety = DestructivePolicy()

    def describe(self):
        return f"add_primary_key:{self.schema}.{self.table}.{self.constraint_name}"

    def apply(self, op: Operations) -> None:
        op.create_primary_key(
            self.constraint_name,
            self.table,
            list(self.columns),
            schema=self.schema,
        )


# ==================================================================================================
# DROP
# ==================================================================================================


@dataclass(frozen=True)
class DropConstraintOp(DbStructureOperation):
    schema: str
    table: str
    constraint_name: str

    priority = 40
    safety = WarningPolicy()

    def describe(self):
        return f"drop_constraint:{self.schema}.{self.table}.{self.constraint_name}"

    def apply(self, op: Operations) -> None:
        op.drop_constraint(
            self.constraint_name,
            self.table,
            schema=self.schema,
        )


@dataclass(frozen=True)
class DropForeignKeyOp(DbStructureOperation):
    schema: str
    table: str
    constraint_name: str

    priority = 45
    safety = DestructivePolicy()

    def describe(self):
        return f"drop_foreignkey:{self.schema}.{self.table}.{self.constraint_name}"

    def apply(self, op: Operations) -> None:
        op.drop_constraint(
            self.constraint_name,
            self.table,
            type_="foreignkey",
            schema=self.schema,
        )


@dataclass(frozen=True)
class DropColumnOp(DbStructureOperation):
    schema: str
    table: str
    column_name: str

    priority = 80
    safety = DestructivePolicy()

    def describe(self):
        return f"drop_column:{self.schema}.{self.table}.{self.column_name}"

    def apply(self, op: Operations) -> None:
        op.drop_column(
            self.table,
            self.column_name,
            schema=self.schema,
        )


@dataclass(frozen=True)
class DropIndexOp(DbStructureOperation):
    schema: str
    table: str
    index_name: str

    priority = 85
    safety = DestructivePolicy()

    def describe(self):
        return f"drop_index:{self.schema}.{self.table}.{self.index_name}"

    def apply(self, op: Operations) -> None:
        op.drop_index(
            self.index_name,
            table_name=self.table,
            schema=self.schema,
        )


@dataclass(frozen=True)
class DropTableOp(DbStructureOperation):
    schema: str
    table: str

    priority = 1000
    safety = DestructivePolicy()

    def describe(self):
        return f"drop_table:{self.schema}.{self.table}"

    def apply(self, op: Operations) -> None:
        op.drop_table(self.table, schema=self.schema)


# ==================================================================================================
# Registry
# ==================================================================================================

ALL_OPERATION_TYPES: tuple[type, ...] = (
    CreateSchemaOp,
    CreateTableOp,
    AddColumnOp,
    CreateIndexOp,
    AddUniqueConstraintOp,
    AddCheckConstraintOp,
    AddExcludeConstraintOp,
    RenameTableOp,
    RenameColumnOp,
    RenameConstraintOp,
    AddForeignKeyOp,
    DropConstraintOp,
    DropForeignKeyOp,
    AlterColumnTypeOp,
    AlterColumnNullabilityOp,
    DropPrimaryKeyOp,
    AddPrimaryKeyOp,
    DropColumnOp,
    DropIndexOp,
    DropTableOp,
)
