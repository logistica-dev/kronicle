# tests/unit/repo/hierarchy/test_hierarchy_repo.py
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm import Session

from kronicle.repo.hierarchy.hierarchy_repo import KronicleHierarchyRepo


class TestRepo(KronicleHierarchyRepo):
    model = MagicMock()
    node_model = MagicMock()


@pytest.fixture
def repo():
    return TestRepo()


@pytest.fixture
def mock_db():
    return MagicMock(spec=Session)


def test_add_parent_calls_ensure_link(repo, mock_db):
    parent, child = MagicMock(id="p"), MagicMock(id="c")
    with patch.object(KronicleHierarchyRepo, "ensure_link") as ensure:
        repo.add_parent(mock_db, parent, child)
    ensure.assert_called_once_with(mock_db, {"parent_id": "p", "child_id": "c"})


def test_remove_parent_calls_remove_link(repo, mock_db):
    parent, child = MagicMock(id="p"), MagicMock(id="c")
    with patch.object(KronicleHierarchyRepo, "remove_link") as remove:
        repo.remove_parent(mock_db, parent, child)
    remove.assert_called_once_with(mock_db, {"parent_id": "p", "child_id": "c"})


def _link(**attrs):
    link = MagicMock()
    for name, value in attrs.items():
        setattr(link, name, value)
    return link


def test_list_parents_returns_link_parents(repo, mock_db):
    link1, link2 = _link(parent="par1"), _link(parent="par2")
    with patch.object(KronicleHierarchyRepo, "list_links", return_value=[link1, link2]) as list_links:
        result = repo.list_parents(mock_db, MagicMock(id="child1"))
    list_links.assert_called_once_with(mock_db, filters={"child_id": "child1"})
    assert result == ["par1", "par2"]


def test_list_children_returns_link_children(repo, mock_db):
    link1, link2 = _link(child="ch1"), _link(child="ch2")
    with patch.object(KronicleHierarchyRepo, "list_links", return_value=[link1, link2]) as list_links:
        result = repo.list_children(mock_db, MagicMock(id="parent1"))
    list_links.assert_called_once_with(mock_db, filters={"parent_id": "parent1"})
    assert result == ["ch1", "ch2"]


def test_list_parents_empty(repo, mock_db):
    with patch.object(KronicleHierarchyRepo, "list_links", return_value=[]) as list_links:
        assert repo.list_parents(mock_db, MagicMock(id="x")) == []


def test_list_children_empty(repo, mock_db):
    with patch.object(KronicleHierarchyRepo, "list_links", return_value=[]) as list_links:
        assert repo.list_children(mock_db, MagicMock(id="x")) == []
