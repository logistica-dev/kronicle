# tests/unit/repo/rbac/links/test_rbac_group_roles_repo.py
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from kronicle.repo.rbac.links.rbac_group_roles_repo import RbacGroupRolesRepository


@pytest.fixture
def repo():
    return RbacGroupRolesRepository()


@pytest.fixture
def mock_db():
    return MagicMock(spec=Session)


class TestGetRoleIdsForGroup:
    def test_returns_set(self, repo, mock_db):
        rid1, rid2 = uuid4(), uuid4()
        mock_db.execute.return_value.scalars.return_value.all.return_value = [rid1, rid2]
        assert repo.get_role_ids_for_group(mock_db, group_id=uuid4()) == {rid1, rid2}


class TestListRolesForGroup:
    def test_returns_roles(self, repo, mock_db):
        roles = [MagicMock(), MagicMock()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = roles
        assert repo.list_roles_for_group(mock_db, group_id=uuid4()) == roles


class TestListRolesForGroups:
    def test_empty_set_returns_empty(self, repo, mock_db):
        assert repo.list_roles_for_groups(mock_db, group_ids=set()) == []
        mock_db.execute.assert_not_called()

    def test_returns_roles(self, repo, mock_db):
        roles = [MagicMock()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = roles
        assert repo.list_roles_for_groups(mock_db, group_ids={uuid4()}) == roles


class TestGetGroupIdsForRole:
    def test_returns_set(self, repo, mock_db):
        gid1 = uuid4()
        mock_db.execute.return_value.scalars.return_value.all.return_value = [gid1]
        assert repo.get_group_ids_for_role(mock_db, role_id=uuid4()) == {gid1}


class TestGetGroupIdsForRoles:
    def test_empty_set_returns_empty(self, repo, mock_db):
        assert repo.get_group_ids_for_roles(mock_db, role_ids=set()) == set()
        mock_db.execute.assert_not_called()

    def test_queries_for_roles(self, repo, mock_db):
        mock_db.execute.return_value.scalars.return_value.all.return_value = [uuid4()]
        result = repo.get_group_ids_for_roles(mock_db, role_ids={uuid4()})
        assert isinstance(result, set)


class TestGetRoleLink:
    def test_returns_first(self, repo, mock_db):
        expected = MagicMock()
        mock_db.execute.return_value.scalars.return_value.first.return_value = expected
        assert repo.get_role_link(mock_db, group_id=uuid4(), role_id=uuid4()) is expected


class TestGetRoleLinkForGroups:
    def test_empty_set_returns_none(self, repo, mock_db):
        assert repo.get_role_link_for_groups(mock_db, group_ids=set(), role_id=uuid4()) is None
        mock_db.execute.assert_not_called()

    def test_returns_first(self, repo, mock_db):
        expected = MagicMock()
        mock_db.execute.return_value.scalars.return_value.first.return_value = expected
        assert repo.get_role_link_for_groups(mock_db, group_ids={uuid4()}, role_id=uuid4()) is expected


class TestAssignRoleToGroup:
    def test_returns_created_row(self, repo, mock_db):
        expected = MagicMock()
        with patch.object(RbacGroupRolesRepository, "ensure_link_returning", return_value=expected) as ensure:
            assert repo.assign_role_to_group(mock_db, group_id=uuid4(), role_id=uuid4()) is expected
        ensure.assert_called_once()


class TestRemoveRoleFromGroup:
    def test_returns_deleted_row(self, repo, mock_db):
        expected = MagicMock()
        with patch.object(RbacGroupRolesRepository, "remove_link_returning", return_value=expected) as remove:
            assert repo.remove_role_from_group(mock_db, group_id=uuid4(), role_id=uuid4()) is expected
        remove.assert_called_once()


class TestDeleteAllForGroup:
    def test_returns_deleted(self, repo, mock_db):
        deleted = [MagicMock()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = deleted
        assert repo.delete_all_for_group(mock_db, group_id=uuid4()) == deleted


class TestDeleteAllForRole:
    def test_returns_deleted(self, repo, mock_db):
        deleted = [MagicMock()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = deleted
        assert repo.delete_all_for_role(mock_db, role_id=uuid4()) == deleted
