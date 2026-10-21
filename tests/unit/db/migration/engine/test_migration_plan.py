# tests/unit/db/migration/engine/test_migration_plan.py
"""Unit tests for kronicle.db.migration.engine.migration_plan.MigrationPlan."""

from dataclasses import dataclass

import pytest

from kronicle.db.migration.engine.migration_plan import MigrationPlan
from kronicle.db.migration.engine.operations import (
    CreateSchemaOp,
    DbStructureOperation,
    DropTableOp,
    SafePolicy,
    SafetyLevel,
)


@dataclass(frozen=True, kw_only=True)
class _NoSchemaOp(DbStructureOperation):
    """An operation carrying no ``schema`` attribute (excluded from ``plan.schemas``)."""

    priority = 1
    safety = SafePolicy()

    def describe(self):
        return "no_schema:op"

    def apply(self, op):
        pass


# ==================================================================================================
# DAG resolution
# ==================================================================================================


def test_build_orders_dependency_after_its_prerequisite():
    base = DropTableOp(schema="core", table="legacy")
    dependent = CreateSchemaOp(schema="core").with_dependency(base)

    plan = MigrationPlan.build([dependent, base])

    assert plan.ordered_operations == [base, dependent]
    assert plan.revision == MigrationPlan.build([base, dependent]).revision


def test_build_obeys_dependencies_over_priority():
    # DropTableOp has priority 1000 vs CreateSchemaOp's 5, but the dependency wins.
    base = DropTableOp(schema="core", table="legacy")
    dependent = CreateSchemaOp(schema="core").with_dependency(base)

    plan = MigrationPlan.build([dependent, base])

    assert [op.op_id for op in plan.ordered_operations] == [base.op_id, dependent.op_id]


def test_build_raises_on_cyclic_dependencies():
    op_a = CreateSchemaOp(schema="core")
    op_b = CreateSchemaOp(schema="rbac")
    cyclic_a = op_a.with_dependency(op_b)
    cyclic_b = op_b.with_dependency(op_a)

    with pytest.raises(RuntimeError, match="Cyclic or unresolved dependencies"):
        MigrationPlan.build([cyclic_a, cyclic_b])


# ==================================================================================================
# Safety classification + queries
# ==================================================================================================


def test_classify_and_query_helpers():
    safe = CreateSchemaOp(schema="core")
    destructive = DropTableOp(schema="core", table="legacy")

    plan = MigrationPlan.build([safe, destructive])

    assert plan.safe_ops() == [safe]
    assert plan.destructive_ops() == [destructive]
    assert plan.warning_ops() == []
    assert plan.by_safety[SafetyLevel.SAFE] == [safe]


# ==================================================================================================
# schemas property
# ==================================================================================================


def test_schemas_collects_scoped_operations_only():
    plan = MigrationPlan.build([CreateSchemaOp(schema="core"), DropTableOp(schema="rbac", table="t"), _NoSchemaOp()])

    assert plan.schemas == {"core", "rbac"}
