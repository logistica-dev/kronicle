# tests/unit/db/core/test_core_models.py
"""Tests for core data models (namespace / table_args helpers)."""

from uuid import uuid4

from kronicle.db.core.links.zone_hierarchy import ZoneHierarchy
from kronicle.db.core.models.core_entity import CoreEntity
from kronicle.db.core.models.core_row import CoreRow


def test_zone_hierarchy_namespace():
    assert ZoneHierarchy.namespace() == "core"


def test_core_entity_namespace_and_table_args():
    assert CoreEntity.namespace() == "core"
    assert CoreEntity.table_args() == {"schema": "core"}


def test_core_row_namespace():
    assert CoreRow.namespace() == "core"
    assert CoreRow.table() == "core.rows"


def test_core_row_snapshot_from_base():
    row = CoreRow(id=uuid4(), name="row-1")
    snap = row.snapshot
    assert snap["name"] == "row-1"
    assert "id" in snap
