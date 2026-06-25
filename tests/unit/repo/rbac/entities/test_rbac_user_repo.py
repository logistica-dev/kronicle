# tests/unit/repo/rbac/entities/test_rbac_user_repo.py
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.errors.error_types import NotFoundError
from kronicle.repo.rbac.entities.rbac_user_repo import RbacUserRepository


@pytest.fixture
def repo():
    return RbacUserRepository()


@pytest.fixture
def mock_db():
    return MagicMock(spec=Session)


def make_user(**overrides):
    user = MagicMock(spec=RbacUser)
    user.id = overrides.get("id", uuid4())
    user.email = overrides.get("email", "user@example.com")
    user.name = overrides.get("name", "test-user")
    user.external_id = overrides.get("external_id", "ext-123")
    user.is_active = overrides.get("is_active", True)
    user.is_superuser = overrides.get("is_superuser", False)
    user.password_hash = overrides.get("password_hash", "hash")
    return user


class TestGetById:
    def test_returns_user_when_found(self, repo, mock_db):
        user = make_user()
        mock_db.execute.return_value.scalar_one_or_none.return_value = user

        result = repo.get_by_id(mock_db, id=user.id)

        assert result is user
        mock_db.execute.assert_called_once()

    def test_returns_none_when_not_found(self, repo, mock_db):
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        result = repo.get_by_id(mock_db, id=uuid4())

        assert result is None

    def test_include_inactive(self, repo, mock_db):
        user = make_user(is_active=False)
        mock_db.execute.return_value.scalar_one_or_none.return_value = user

        result = repo.get_by_id(mock_db, id=user.id, include_inactive=True)

        assert result is user

    def test_include_superusers(self, repo, mock_db):
        user = make_user(is_superuser=True)
        mock_db.execute.return_value.scalar_one_or_none.return_value = user

        result = repo.get_by_id(mock_db, id=user.id, include_superusers=True)

        assert result is user


class TestGetByEmail:
    def test_returns_user_when_found(self, repo, mock_db):
        user = make_user()
        mock_db.execute.return_value.scalar_one_or_none.return_value = user

        result = repo.get_by_email(mock_db, email=user.email)

        assert result is user
        mock_db.execute.assert_called_once()

    def test_returns_none_when_not_found(self, repo, mock_db):
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        result = repo.get_by_email(mock_db, email="missing@example.com")

        assert result is None


class TestGetByName:
    def test_returns_user_when_found(self, repo, mock_db):
        user = make_user()
        mock_db.execute.return_value.scalar_one_or_none.return_value = user

        result = repo.get_by_name(mock_db, name=user.name)

        assert result is user
        mock_db.execute.assert_called_once()

    def test_returns_none_when_not_found(self, repo, mock_db):
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        result = repo.get_by_name(mock_db, name="missing-user")

        assert result is None


class TestGetByExternalId:
    def test_returns_user_when_found(self, repo, mock_db):
        user = make_user()
        mock_db.execute.return_value.scalar_one_or_none.return_value = user

        result = repo.get_by_external_id(mock_db, external_id=user.external_id)

        assert result is user
        mock_db.execute.assert_called_once()

    def test_returns_none_when_not_found(self, repo, mock_db):
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        result = repo.get_by_external_id(mock_db, external_id="missing-ext")

        assert result is None


class TestFetchAll:
    def test_returns_all_users(self, repo, mock_db):
        users = [make_user(), make_user()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = users

        result = repo.fetch_all(mock_db)

        assert result == users
        mock_db.execute.assert_called_once()

    def test_include_inactive(self, repo, mock_db):
        users = [make_user(is_active=False)]
        mock_db.execute.return_value.scalars.return_value.all.return_value = users

        result = repo.fetch_all(mock_db, include_inactive=True)

        assert result == users

    def test_include_superusers(self, repo, mock_db):
        users = [make_user(is_superuser=True)]
        mock_db.execute.return_value.scalars.return_value.all.return_value = users

        result = repo.fetch_all(mock_db, include_superusers=True)

        assert result == users


class TestCreateUser:
    def test_calls_add(self, repo, mock_db):
        user = make_user()

        result = repo.create_user(mock_db, user=user)

        assert result is user


class TestUpdateUser:
    def test_calls_save(self, repo, mock_db):
        user = make_user()

        result = repo.update_user(mock_db, user=user)

        assert result is user


class TestUpdatePasswordHash:
    def test_updates_hash_when_user_found(self, repo, mock_db):
        user = make_user()
        mock_db.get.return_value = user
        new_hash = "new-hash-value"

        repo.update_password_hash(mock_db, user_id=user.id, new_hash=new_hash)

        assert user.password_hash == new_hash
        mock_db.get.assert_called_once_with(RbacUser, user.id)

    def test_raises_not_found_when_user_missing(self, repo, mock_db):
        mock_db.get.return_value = None

        with pytest.raises(NotFoundError, match="User not found"):
            repo.update_password_hash(mock_db, user_id=uuid4(), new_hash="hash")


class TestDeleteUser:
    def test_calls_delete_by_id_returning(self, repo, mock_db):
        user = make_user()
        mock_db.execute.return_value.scalar_one_or_none.return_value = user

        result = repo.delete_user(mock_db, user=user)

        assert result is user
