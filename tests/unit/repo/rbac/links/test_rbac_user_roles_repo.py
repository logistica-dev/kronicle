# tests/unit/repo/rbac/links/test_rbac_user_roles_repo.py
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from kronicle.repo.rbac.links.rbac_user_roles_repo import RbacUserRolesRepository


@pytest.fixture
def repo():
    return RbacUserRolesRepository()


@pytest.fixture
def mock_db():
    return MagicMock(spec=Session)


class TestGetRoleIdsForUser:
    def test_returns_set_of_role_ids(self, repo, mock_db):
        role_id1, role_id2 = uuid4(), uuid4()
        mock_db.execute.return_value.scalars.return_value.all.return_value = [role_id1, role_id2]
        result = repo.get_role_ids_for_user(mock_db, user_id=uuid4())
        assert result == {role_id1, role_id2}


class TestListRolesForUser:
    def test_returns_roles(self, repo, mock_db):
        roles = [MagicMock(), MagicMock()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = roles
        assert repo.list_roles_for_user(mock_db, user_id=uuid4()) == roles


class TestListUserForRole:
    def test_returns_users(self, repo, mock_db):
        users = [MagicMock(), MagicMock()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = users
        assert repo.list_user_for_role(mock_db, role_id=uuid4()) == users


class TestGetUserIdsForRole:
    def test_returns_set(self, repo, mock_db):
        uid1, uid2 = uuid4(), uuid4()
        mock_db.execute.return_value.scalars.return_value.all.return_value = [uid1, uid2]
        assert repo.get_user_ids_for_role(mock_db, role_id=uuid4()) == {uid1, uid2}


class TestGetUserIdsForRoles:
    def test_empty_set_returns_empty(self, repo, mock_db):
        assert repo.get_user_ids_for_roles(mock_db, role_ids=set()) == set()
        mock_db.execute.assert_not_called()

    def test_queries_for_roles(self, repo, mock_db):
        role_ids = {uuid4(), uuid4()}
        mock_db.execute.return_value.scalars.return_value.all.return_value = [uuid4()]
        result = repo.get_user_ids_for_roles(mock_db, role_ids=role_ids)
        assert isinstance(result, set)


class TestGetRoleLink:
    def test_returns_first(self, repo, mock_db):
        expected = MagicMock()
        mock_db.execute.return_value.scalars.return_value.first.return_value = expected
        assert repo.get_role_link(mock_db, user_id=uuid4(), role_id=uuid4()) is expected


class TestAssignRoleToUser:
    def test_returns_created_row(self, repo, mock_db):
        expected = MagicMock()
        with patch.object(RbacUserRolesRepository, "ensure_link_returning", return_value=expected) as ensure:
            result = repo.assign_role_to_user(mock_db, user_id=uuid4(), role_id=uuid4())
        assert result is expected
        ensure.assert_called_once()


class TestRemoveRoleFromUser:
    def test_returns_deleted_row(self, repo, mock_db):
        expected = MagicMock()
        with patch.object(RbacUserRolesRepository, "remove_link_returning", return_value=expected) as remove:
            result = repo.remove_role_from_user(mock_db, user_id=uuid4(), role_id=uuid4())
        assert result is expected
        remove.assert_called_once()


class TestDeleteAllForUser:
    def test_returns_deleted(self, repo, mock_db):
        deleted = [MagicMock()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = deleted
        assert repo.delete_all_for_user(mock_db, user_id=uuid4()) == deleted


class TestDeleteAllForRole:
    def test_returns_deleted(self, repo, mock_db):
        deleted = [MagicMock()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = deleted
        assert repo.delete_all_for_role(mock_db, role_id=uuid4()) == deleted
