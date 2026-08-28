# tests/unit/repo/hierarchy/test_hierarchy_service.py
from typing import cast
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from kronicle.repo.hierarchy.hierarchy_repo import KronicleHierarchyRepo
from kronicle.repo.hierarchy.hierarchy_service import HierarchyService


class FakeRepo:
    def __init__(self, parents=None, children=None):
        self.parents = parents or {}
        self.children = children or {}
        self.calls = []

    def list_parents(self, db, node):
        self.calls.append((db, "list_parents", node.id))
        return list(self.parents.get(node.id, []))

    def list_children(self, db, node):
        self.calls.append((db, "list_children", node.id))
        return list(self.children.get(node.id, []))

    def add_parent(self, db, parent, child):
        self.calls.append((db, "add_parent", parent.id, child.id))

    def remove_parent(self, db, parent, child):
        self.calls.append((db, "remove_parent", parent.id, child.id))


def make_node(id=None):
    node = MagicMock()
    node.id = id or uuid4()
    return node


def make_repo(parents=None, children=None) -> KronicleHierarchyRepo:
    return cast(KronicleHierarchyRepo, FakeRepo(parents=parents, children=children))


class TestAddParent:
    def test_uses_caller_session_for_checks_and_insert(self):
        session = MagicMock()
        repo = make_repo()
        service = HierarchyService(repo=repo, max_parents=1)
        parent = make_node()
        child = make_node()

        service.add_parent(session, parent, child)

        assert (session, "add_parent", parent.id, child.id) in repo.calls
        assert all(call[0] is session for call in repo.calls)

    def test_raises_on_cycle(self):
        a, b, c = make_node(), make_node(), make_node()
        repo = make_repo(children={c.id: [b], b.id: [a]})
        service = HierarchyService(repo=repo, max_parents=1)

        with pytest.raises(ValueError, match="Cycle detected"):
            service.add_parent(MagicMock(), a, c)

        assert not any(call[1] == "add_parent" for call in repo.calls)

    def test_raises_on_max_parents_exceeded(self):
        child = make_node()
        repo = make_repo(parents={child.id: [make_node()]})
        service = HierarchyService(repo=repo, max_parents=1)

        with pytest.raises(ValueError, match="Max parents exceeded"):
            service.add_parent(MagicMock(), make_node(), child)

        assert not any(call[1] == "add_parent" for call in repo.calls)

    def test_no_parent_limit_when_max_parents_is_none(self):
        session = MagicMock()
        child = make_node()
        parent = make_node()
        repo = make_repo(parents={child.id: [make_node()]})
        service = HierarchyService(repo=repo, max_parents=None)

        service.add_parent(session, parent, child)

        assert (session, "add_parent", parent.id, child.id) in repo.calls


class TestRemoveParent:
    def test_removes_edge(self):
        session = MagicMock()
        repo = make_repo()
        service = HierarchyService(repo=repo)
        parent = make_node()
        child = make_node()

        service.remove_parent(session, parent, child)

        assert (session, "remove_parent", parent.id, child.id) in repo.calls


class TestAncestors:
    def test_returns_ancestors(self):
        a, b = make_node(), make_node()
        repo = make_repo(parents={b.id: [a]})
        service = HierarchyService(repo=repo)

        assert service.ancestors(MagicMock(), b) == [a]

    def test_ancestors_ids(self):
        a, b = make_node(), make_node()
        repo = make_repo(parents={b.id: [a]})
        service = HierarchyService(repo=repo)

        assert service.ancestors_ids(MagicMock(), b) == {a.id}


class TestDescendants:
    def test_returns_descendants(self):
        a, b = make_node(), make_node()
        repo = make_repo(children={a.id: [b]})
        service = HierarchyService(repo=repo)

        assert service.descendants(MagicMock(), a) == [b]

    def test_descendants_ids(self):
        a, b = make_node(), make_node()
        repo = make_repo(children={a.id: [b]})
        service = HierarchyService(repo=repo)

        assert service.descendants_ids(MagicMock(), a) == {b.id}


class TestDescendantClosure:
    def test_returns_all_nodes_and_descendants(self):
        a, b, c = make_node(), make_node(), make_node()
        repo = make_repo(children={a.id: [b, c]})
        service = HierarchyService(repo=repo)

        result = service.descendant_closure(MagicMock(), [a])

        assert result == {a, b, c}

    def test_uses_single_session_for_all_traversal(self):
        session = MagicMock()
        a, b = make_node(), make_node()
        repo = make_repo(children={a.id: [b]})
        service = HierarchyService(repo=repo)

        service.descendant_closure(session, [a])

        assert all(call[0] is session for call in repo.calls)
