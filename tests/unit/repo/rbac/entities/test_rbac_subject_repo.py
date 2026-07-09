# tests/unit/repo/rbac/entities/test_rbac_subject_repo.py
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_subject import RbacSubject
from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.repo.rbac.entities.rbac_subject_repo import RbacSubjectRepository


@pytest.fixture
def repo():
    return RbacSubjectRepository()


@pytest.fixture
def mock_db():
    return MagicMock(spec=Session)


def make_user(**overrides):
    user = MagicMock(spec=RbacUser)
    user.id = overrides.get("id", uuid4())
    user.name = overrides.get("name", "test-user")
    return user


def make_group(**overrides):
    group = MagicMock(spec=RbacGroup)
    group.id = overrides.get("id", uuid4())
    group.name = overrides.get("name", "test-group")
    return group


class TestEnsureFromUser:
    def test_returns_existing_subject(self, repo, mock_db):
        user = make_user()
        existing = MagicMock(spec=RbacSubject)
        repo.get_by_id = MagicMock(return_value=existing)

        result = repo.ensure_from_user(mock_db, user=user)

        assert result is existing
        repo.get_by_id.assert_called_once_with(mock_db, id=user.id)

    def test_creates_new_subject_when_not_found(self, repo, mock_db):
        user = make_user()
        repo.get_by_id = MagicMock(return_value=None)
        repo.add = MagicMock(return_value=None)

        result = repo.ensure_from_user(mock_db, user=user)

        assert isinstance(result, RbacSubject)
        assert result.id == user.id
        assert result.type == "user"
        assert result.user_id == user.id
        assert result.name == user.name
        repo.get_by_id.assert_called_once_with(mock_db, id=user.id)
        repo.add.assert_called_once_with(mock_db, entity=result)


class TestEnsureFromGroup:
    def test_returns_existing_subject(self, repo, mock_db):
        group = make_group()
        existing = MagicMock(spec=RbacSubject)
        repo.get_by_id = MagicMock(return_value=existing)

        result = repo.ensure_from_group(mock_db, group=group)

        assert result is existing
        repo.get_by_id.assert_called_once_with(mock_db, id=group.id)

    def test_creates_new_subject_when_not_found(self, repo, mock_db):
        group = make_group()
        repo.get_by_id = MagicMock(return_value=None)
        repo.add = MagicMock(return_value=None)

        result = repo.ensure_from_group(mock_db, group=group)

        assert isinstance(result, RbacSubject)
        assert result.id == group.id
        assert result.type == "group"
        assert result.group_id == group.id
        assert result.name == group.name
        repo.get_by_id.assert_called_once_with(mock_db, id=group.id)
        repo.add.assert_called_once_with(mock_db, entity=result)
