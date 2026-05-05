# kronicle/db/migration/operations.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from typing import FrozenSet, Tuple

from alembic.operations import Operations
from sqlalchemy import Column
from sqlalchemy.types import TypeEngine

# ==================================================================================================
# Safety model
# ==================================================================================================


class SafetyLevel:
    SAFE = "safe"
    WARNING = "warning"
    DESTRUCTIVE = "destructive"


# ==================================================================================================
# Base operation
# ==================================================================================================


@dataclass(frozen=True, kw_only=True)
class SchemaOperation(ABC):
    """
    Immutable migration intent.

    Design goals:
    - deterministic diff representation
    - dependency-safe (DAG friendly)
    - directly executable via Alembic
    """

    priority: int = 100
    safety: str = SafetyLevel.SAFE

    # dependency graph (by op_id, not object references)
    depends_on: FrozenSet[str] = field(default_factory=frozenset, repr=False)

    # ----------------------------------------------------------------------------------------------
    # Identity
    # ----------------------------------------------------------------------------------------------
    @property
    def op_id(self) -> str:
        return self.describe()

    # ----------------------------------------------------------------------------------------------
    # Dependency handling
    # ----------------------------------------------------------------------------------------------
    def with_dependency(self, other: SchemaOperation) -> SchemaOperation:
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
class CreateSchemaOp(SchemaOperation):
    schema: str = ""
    priority: int = 5

    def describe(self):
        return f"create_schema:{self.schema}"

    def apply(self, op: Operations) -> None:
        op.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")


@dataclass(frozen=True)
class CreateTableOp(SchemaOperation):
    schema: str
    table: str
    columns: Tuple = field(default_factory=tuple)
    priority: int = 10

    def describe(self):
        return f"create_table:{self.schema}.{self.table}"

    def apply(self, op: Operations) -> None:
        op.create_table(
            self.table,
            *self.columns,
            schema=self.schema,
        )


@dataclass(frozen=True)
class AddColumnOp(SchemaOperation):
    schema: str
    table: str
    column_name: str
    column_def: Column  # SQLAlchemy Column
    priority: int = 20

    def describe(self):
        return f"add_column:{self.schema}.{self.table}.{self.column_name}"

    def apply(self, op: Operations) -> None:
        op.add_column(
            self.table,
            self.column_def,
            schema=self.schema,
        )


@dataclass(frozen=True)
class AddUniqueConstraintOp(SchemaOperation):
    schema: str
    table: str
    constraint_name: str
    columns: tuple

    def describe(self):
        return f"add_unique:{self.schema}.{self.table}.{self.constraint_name}"

    def apply(self, op: Operations) -> None:
        op.create_unique_constraint(
            self.constraint_name,
            self.table,
            *self.columns,
            schema=self.schema,
        )


@dataclass(frozen=True)
class AddCheckConstraintOp(SchemaOperation):
    schema: str
    table: str
    constraint_name: str
    sqltext: str

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
class AddExcludeConstraintOp(SchemaOperation):
    schema: str
    table: str
    constraint_name: str
    elements: tuple

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
class RenameTableOp(SchemaOperation):
    schema: str
    old_name: str
    new_name: str
    priority: int = 15

    def describe(self):
        return f"rename_table:{self.schema}.{self.old_name}->{self.new_name}"

    def apply(self, op: Operations) -> None:
        op.rename_table(
            self.old_name,
            self.new_name,
            schema=self.schema,
        )


@dataclass(frozen=True)
class RenameColumnOp(SchemaOperation):
    schema: str
    table: str
    old_name: str
    new_name: str
    priority: int = 50

    def describe(self):
        return f"rename_column:{self.schema}.{self.table}.{self.old_name}->{self.new_name}"

    def apply(self, op: Operations) -> None:
        op.alter_column(
            self.table,
            self.old_name,
            new_column_name=self.new_name,
            schema=self.schema,
        )


# ==================================================================================================
# ALTER
# ==================================================================================================


@dataclass(frozen=True)
class AlterColumnTypeOp(SchemaOperation):
    schema: str
    table: str
    column: str
    old_type: TypeEngine
    new_type: TypeEngine
    priority: int = 60
    safety: str = SafetyLevel.WARNING

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
class AlterColumnNullabilityOp(SchemaOperation):
    schema: str
    table: str
    column: str
    nullable: bool
    priority: int = 55
    safety: str = SafetyLevel.WARNING

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
# DROP
# ==================================================================================================


@dataclass(frozen=True)
class DropConstraintOp(SchemaOperation):
    schema: str
    table: str
    constraint_name: str
    priority: int = 40
    safety: str = SafetyLevel.WARNING

    def describe(self):
        return f"drop_constraint:{self.schema}.{self.table}.{self.constraint_name}"

    def apply(self, op: Operations) -> None:
        op.drop_constraint(
            self.constraint_name,
            self.table,
            schema=self.schema,
        )


@dataclass(frozen=True)
class DropColumnOp(SchemaOperation):
    schema: str
    table: str
    column_name: str
    priority: int = 80
    safety: str = SafetyLevel.DESTRUCTIVE

    def describe(self):
        return f"drop_column:{self.schema}.{self.table}.{self.column_name}"

    def apply(self, op: Operations) -> None:
        op.drop_column(
            self.table,
            self.column_name,
            schema=self.schema,
        )


@dataclass(frozen=True)
class DropTableOp(SchemaOperation):
    schema: str
    table: str
    priority: int = 1000
    safety: str = SafetyLevel.DESTRUCTIVE

    def describe(self):
        return f"drop_table:{self.schema}.{self.table}"

    def apply(self, op: Operations) -> None:
        op.drop_table(
            self.table,
            schema=self.schema,
        )


# ==================================================================================================
# Registry
# ==================================================================================================

ALL_OPERATION_TYPES: Tuple[type, ...] = (
    CreateSchemaOp,
    CreateTableOp,
    AddColumnOp,
    AddUniqueConstraintOp,
    AddCheckConstraintOp,
    AddExcludeConstraintOp,
    RenameTableOp,
    RenameColumnOp,
    AlterColumnTypeOp,
    AlterColumnNullabilityOp,
    DropConstraintOp,
    DropColumnOp,
    DropTableOp,
)
