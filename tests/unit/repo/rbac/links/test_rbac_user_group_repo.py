# tests/unit/repo/rbac/links/test_rbac_user_group_repo.py
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from kronicle.repo.rbac.links.rbac_user_group_repo import RbacUserGroupRepository


@pytest.fixture
def repo():
    return RbacUserGroupRepository()


@pytest.fixture
def mock_db():
    return MagicMock(spec=Session)


class TestGetGroupIdsForUser:
    def test_returns_set(self, repo, mock_db):
        gid1, gid2 = uuid4(), uuid4()
        mock_db.execute.return_value.scalars.return_value.all.return_value = [gid1, gid2]
        assert repo.get_group_ids_for_user(mock_db, user_id=uuid4()) == {gid1, gid2}


class TestListGroupsForUser:
    def test_returns_groups(self, repo, mock_db):
        groups = [MagicMock(), MagicMock()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = groups
        assert repo.list_groups_for_user(mock_db, user_id=uuid4()) == groups


class TestListUsersForGroup:
    def test_returns_users(self, repo, mock_db):
        users = [MagicMock(), MagicMock()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = users
        assert repo.list_users_for_group(mock_db, group_id=uuid4()) == users


class TestGetUserIdsForGroup:
    def test_returns_set(self, repo, mock_db):
        uid1 = uuid4()
        mock_db.execute.return_value.scalars.return_value.all.return_value = [uid1]
        assert repo.get_user_ids_for_group(mock_db, group_id=uuid4()) == {uid1}


class TestGetUserIdsForGroups:
    def test_empty_set_returns_empty(self, repo, mock_db):
        assert repo.get_user_ids_for_groups(mock_db, group_ids=set()) == set()
        mock_db.execute.assert_not_called()

    def test_queries_for_groups(self, repo, mock_db):
        mock_db.execute.return_value.scalars.return_value.all.return_value = [uuid4()]
        result = repo.get_user_ids_for_groups(mock_db, group_ids={uuid4()})
        assert isinstance(result, set)


class TestGetMembershipLink:
    def test_returns_first(self, repo, mock_db):
        expected = MagicMock()
        mock_db.execute.return_value.scalars.return_value.first.return_value = expected
        assert repo.get_membership_link(mock_db, user_id=uuid4(), group_id=uuid4()) is expected


class TestAddUserToGroup:
    def test_returns_created_row(self, repo, mock_db):
        expected = MagicMock()
        user, group = SimpleNamespace(id=uuid4()), SimpleNamespace(id=uuid4())
        with patch.object(RbacUserGroupRepository, "ensure_link_returning", return_value=expected) as ensure:
            assert repo.add_user_to_group(mock_db, user=user, group=group) is expected
        ensure.assert_called_once()


class TestRemoveUserFromGroup:
    def test_returns_deleted_row(self, repo, mock_db):
        expected = MagicMock()
        user, group = SimpleNamespace(id=uuid4()), SimpleNamespace(id=uuid4())
        with patch.object(RbacUserGroupRepository, "remove_link_returning", return_value=expected) as remove:
            assert repo.remove_user_from_group(mock_db, user=user, group=group) is expected
        remove.assert_called_once()


class TestDeleteAllForUser:
    def test_returns_deleted(self, repo, mock_db):
        deleted = [MagicMock()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = deleted
        assert repo.delete_all_for_user(mock_db, user_id=uuid4()) == deleted


class TestDeleteAllForGroup:
    def test_returns_deleted(self, repo, mock_db):
        deleted = [MagicMock()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = deleted
        assert repo.delete_all_for_group(mock_db, group_id=uuid4()) == deleted
