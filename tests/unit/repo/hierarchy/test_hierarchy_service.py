# tests/unit/repo/hierarchy/test_hierarchy_service.py
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from kronicle.repo.hierarchy.hierarchy_service import HierarchyService


@pytest.fixture
def engine():
    return MagicMock()


@pytest.fixture
def add_edge():
    return MagicMock()


@pytest.fixture
def remove_edge():
    return MagicMock()


@pytest.fixture
def session():
    return MagicMock(spec=Session)


def make_node(id=None):
    node = MagicMock()
    node.id = id or uuid4()
    return node


class TestAddParent:
    def test_adds_edge_when_valid(self, engine, add_edge, remove_edge, session):
        service = HierarchyService(engine, add_edge, remove_edge, max_parents=1)
        parent = make_node()
        child = make_node()
        engine.would_create_cycle.return_value = False
        engine.parents_of.return_value = []

        service.add_parent(session, parent, child)

        add_edge.assert_called_once_with(session, parent, child)

    def test_raises_on_cycle(self, engine, add_edge, remove_edge, session):
        service = HierarchyService(engine, add_edge, remove_edge, max_parents=1)
        parent = make_node()
        child = make_node()
        engine.would_create_cycle.return_value = True

        with pytest.raises(ValueError, match="Cycle detected"):
            service.add_parent(session, parent, child)

        add_edge.assert_not_called()

    def test_raises_on_max_parents_exceeded(self, engine, add_edge, remove_edge, session):
        service = HierarchyService(engine, add_edge, remove_edge, max_parents=1)
        parent = make_node()
        child = make_node()
        engine.would_create_cycle.return_value = False
        engine.parents_of.return_value = [make_node()]

        with pytest.raises(ValueError, match="Max parents exceeded"):
            service.add_parent(session, parent, child)

        add_edge.assert_not_called()

    def test_no_parent_limit_when_max_parents_is_none(self, engine, add_edge, remove_edge, session):
        service = HierarchyService(engine, add_edge, remove_edge, max_parents=None)
        parent = make_node()
        child = make_node()
        engine.would_create_cycle.return_value = False

        service.add_parent(session, parent, child)

        add_edge.assert_called_once_with(session, parent, child)


class TestRemoveParent:
    def test_removes_edge(self, engine, add_edge, remove_edge, session):
        service = HierarchyService(engine, add_edge, remove_edge)
        parent = make_node()
        child = make_node()

        service.remove_parent(session, parent, child)

        remove_edge.assert_called_once_with(session, parent, child)


class TestAncestors:
    def test_delegates_to_engine(self, engine, add_edge, remove_edge, session):
        service = HierarchyService(engine, add_edge, remove_edge, max_parents=1)
        node = make_node()
        expected = [make_node(), make_node()]
        engine.ancestors.return_value = iter(expected)

        result = service.ancestors(node)

        assert result == expected
        engine.ancestors.assert_called_once_with(node)


class TestDescendants:
    def test_delegates_to_engine(self, engine, add_edge, remove_edge, session):
        service = HierarchyService(engine, add_edge, remove_edge, max_parents=1)
        node = make_node()
        expected = [make_node(), make_node()]
        engine.descendants.return_value = iter(expected)

        result = service.descendants(node)

        assert result == expected
        engine.descendants.assert_called_once_with(node)


class TestDescendantClosure:
    def test_returns_all_nodes_and_descendants(self, engine, add_edge, remove_edge, session):
        service = HierarchyService(engine, add_edge, remove_edge, max_parents=1)
        node1 = make_node()
        node2 = make_node()
        desc1 = make_node()
        desc2 = make_node()
        engine.descendants.side_effect = [
            iter([desc1]),
            iter([desc2]),
        ]

        result = service.descendant_closure([node1, node2])

        assert result == {node1, node2, desc1, desc2}
