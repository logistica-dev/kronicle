# tests/unit/services/test_rbac_users.py
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from kronicle.errors.error_types import BadRequestError, NotFoundError, UnauthorizedError
from kronicle.schemas.rbac.input_user_schemas import InputUserLogin
from kronicle.schemas.rbac.safe_user_schemas import OutputUser, ProcessedUser
from tests.unit.services.conftest import fake_group, fake_user


class TestUserRead:
    def test_fetch_user_for_auth(self, rbac_service):
        db_user = fake_user()
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        login = MagicMock(spec=InputUserLogin)
        login.is_email = True
        login.login = "u@k.app"

        result = rbac_service.fetch_user_for_auth(login)
        assert result is db_user

    def test_fetch_user_for_auth_by_name(self, rbac_service):
        db_user = fake_user(name="testuser")
        rbac_service._user_repo.get_by_name = MagicMock(return_value=db_user)
        login = MagicMock(spec=InputUserLogin)
        login.is_email = False
        login.login = "testuser"

        result = rbac_service.fetch_user_for_auth(login)
        assert result is db_user

    def test_fetch_user_for_auth_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_email = MagicMock(return_value=None)
        login = MagicMock(spec=InputUserLogin)
        login.is_email = True
        login.login = "noone@k.app"

        with pytest.raises(NotFoundError, match="User not found"):
            rbac_service.fetch_user_for_auth(login)

    def test_fetch_user_info(self, rbac_service):
        db_user = fake_user()
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        login = MagicMock(spec=InputUserLogin)
        login.is_email = True
        login.login = "u@k.app"

        result = rbac_service.fetch_user_info(login)
        assert isinstance(result, OutputUser)
        assert result.email == db_user.email

    def test_fetch_user_by_email(self, rbac_service):
        db_user = fake_user(email="found@k.app")
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)

        result = rbac_service.fetch_user_by_email("found@k.app")
        assert isinstance(result, OutputUser)
        assert result.email == "found@k.app"

    def test_fetch_user_by_email_none(self, rbac_service):
        rbac_service._user_repo.get_by_email = MagicMock(return_value=None)
        assert rbac_service.fetch_user_by_email("noone@k.app") is None

    def test_fetch_user_by_name(self, rbac_service):
        db_user = fake_user(name="bob")
        rbac_service._user_repo.get_by_name = MagicMock(return_value=db_user)

        result = rbac_service.fetch_user_by_name("bob")
        assert isinstance(result, OutputUser)
        assert result.name == "bob"

    def test_fetch_user_by_name_none(self, rbac_service):
        rbac_service._user_repo.get_by_name = MagicMock(return_value=None)
        assert rbac_service.fetch_user_by_name("noone") is None

    def test_fetch_user_by_id(self, rbac_service):
        uid = uuid4()
        db_user = fake_user(id=uid)
        rbac_service._user_repo.get_by_id = MagicMock(return_value=db_user)

        result = rbac_service.fetch_user_by_id(uid)
        assert isinstance(result, OutputUser)
        assert result.id == uid

    def test_fetch_user_by_id_none(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.fetch_user_by_id(uuid4()) is None

    def test_fetch_user_by_external_id(self, rbac_service):
        db_user = fake_user()
        db_user.external_id = "ext-123"
        rbac_service._user_repo.get_by_external_id = MagicMock(return_value=db_user)

        result = rbac_service.fetch_user_by_external_id("ext-123")
        assert isinstance(result, OutputUser)

    def test_fetch_user_by_external_id_none(self, rbac_service):
        rbac_service._user_repo.get_by_external_id = MagicMock(return_value=None)
        assert rbac_service.fetch_user_by_external_id("ext-x") is None

    def test_list_users(self, rbac_service):
        rbac_service._user_repo.fetch_all = MagicMock(return_value=[fake_user(), fake_user()])
        result = rbac_service.list_users()
        assert len(result) == 2
        assert all(isinstance(u, OutputUser) for u in result)


# ==================================================================================================
# User write methods
# ==================================================================================================


class TestUserWrite:
    def test_create_user_no_password_hash(self, rbac_service):
        user = ProcessedUser(email="nopass@k.app", name="NoPass")
        with pytest.raises(BadRequestError, match="password"):
            rbac_service.create_user(user)

    def test_create_user_success(self, rbac_service):
        user = ProcessedUser(email="new@k.app", name="NewUser", password_hash="h")
        db_user = fake_user(id=uuid4(), name="New", email="new@k.app")
        rbac_service._user_repo.get_by_email = MagicMock(return_value=None)
        rbac_service._user_repo.create_user = MagicMock(return_value=db_user)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)

        out = rbac_service.create_user(user)
        assert isinstance(out, OutputUser)
        assert out.email == user.email

    def test_create_user_already_exists(self, rbac_service):
        user = ProcessedUser(email="dup@k.app", name="DupUser", password_hash="h")
        rbac_service._user_repo.get_by_email = MagicMock(return_value=fake_user())
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        with pytest.raises(UnauthorizedError, match="Email already in use"):
            rbac_service.create_user(user)

    def test_patch_user(self, rbac_service):
        db_user = fake_user(email="old@k.app", name="old")
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        user = ProcessedUser(email="old@k.app", name="newname", password_hash="h")

        out = rbac_service.patch_user(user)
        assert out.name == "newname"
        assert db_user.name == "newname"

    def test_patch_user_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_email = MagicMock(return_value=None)
        user = ProcessedUser(email="noone@k.app", name="x_user", password_hash="h")
        with pytest.raises(UnauthorizedError, match="doesn't exists"):
            rbac_service.patch_user(user)

    def test_patch_user_integrity_error(self, rbac_service):
        from sqlalchemy.exc import IntegrityError

        db_user = fake_user(email="u@k.app", name="u")
        db_user.full_name = None
        db_user.external_id = None
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        db.commit.side_effect = IntegrityError("stmt", {}, BaseException())

        user = ProcessedUser(email="u@k.app", name="newname", password_hash="h")
        with pytest.raises(UnauthorizedError, match="existing values"):
            rbac_service.patch_user(user)

    def test_patch_user_no_changes(self, rbac_service):
        db_user = fake_user(email="u@k.app", name="u")
        db_user.full_name = None
        db_user.external_id = None
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        user = ProcessedUser(email="u@k.app", name="uname", password_hash="h")

        out = rbac_service.patch_user(user)
        assert out.name == "uname"

    def test_patch_user_by_id(self, rbac_service):
        uid = uuid4()
        db_user = fake_user(id=uid, name="old")
        rbac_service._user_repo.get_by_id = MagicMock(return_value=db_user)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)

        out = rbac_service.patch_user_by_id(uid, name="newname", full_name="Full", orcid="ext-1")
        assert out.name == "newname"
        assert db_user.name == "newname"
        assert db_user.full_name == "Full"
        assert db_user.external_id == "ext-1"

    def test_patch_user_by_id_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_user_by_id(uuid4(), name="x")

    def test_patch_user_by_id_integrity_error(self, rbac_service):
        from sqlalchemy.exc import IntegrityError

        uid = uuid4()
        db_user = fake_user(id=uid, name="old")
        rbac_service._user_repo.get_by_id = MagicMock(return_value=db_user)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        db.commit.side_effect = IntegrityError("stmt", {}, BaseException())

        with pytest.raises(BadRequestError, match="existing values"):
            rbac_service.patch_user_by_id(uid, name="newname")

    def test_patch_user_by_id_no_changes(self, rbac_service):
        uid = uuid4()
        db_user = fake_user(id=uid, name="u")
        rbac_service._user_repo.get_by_id = MagicMock(return_value=db_user)

        out = rbac_service.patch_user_by_id(uid)
        assert out.name == "u"

    def test_update_password_hash(self, rbac_service):
        rbac_service._user_repo.update_password_hash = MagicMock()
        rbac_service.update_password_hash(uuid4(), "new_hash")
        rbac_service._user_repo.update_password_hash.assert_called_once()

    def test_deactivate_user(self, rbac_service):
        db_user = fake_user(name="active")
        db_user.is_active = True
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        user = ProcessedUser(email="active@k.app", name="active", password_hash="h")

        out = rbac_service.deactivate_user(user)
        assert out is not None
        assert db_user.is_active is False

    def test_deactivate_user_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_email = MagicMock(return_value=None)
        user = ProcessedUser(email="noone@k.app", name="x_user", password_hash="h")
        with pytest.raises(UnauthorizedError):
            rbac_service.deactivate_user(user)

    def test_deactivate_user_by_id(self, rbac_service):
        uid = uuid4()
        db_user = fake_user(id=uid, name="active")
        db_user.is_active = True
        rbac_service._user_repo.get_by_id = MagicMock(return_value=db_user)

        out = rbac_service.deactivate_user_by_id(uid)
        assert out is not None
        assert db_user.is_active is False

    def test_deactivate_user_by_id_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(UnauthorizedError):
            rbac_service.deactivate_user_by_id(uuid4())

    def test_remove_user(self, rbac_service):
        db_user = fake_user(name="del")
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        rbac_service._user_repo.delete_user = MagicMock(return_value=db_user)
        user = ProcessedUser(email="del@k.app", name="deluser", password_hash="h")

        out = rbac_service.remove_user(user)
        assert out is not None

    def test_remove_user_by_id(self, rbac_service):
        uid = uuid4()
        db_user = fake_user(id=uid, name="del")
        rbac_service._user_repo.get_by_id = MagicMock(return_value=db_user)
        rbac_service._user_repo.delete_user = MagicMock(return_value=db_user)

        out = rbac_service.remove_user_by_id(uid)
        assert out is not None


# ==================================================================================================
# Group methods
# ==================================================================================================


class TestNameReservation:
    def test_reserved_name_user(self, rbac_service):
        user = ProcessedUser(email="admin@k.app", name="admin", password_hash="h")
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        with pytest.raises(BadRequestError, match="reserved"):
            rbac_service.create_user(user)

    def test_reserved_name_group(self, rbac_service):
        rbac_service._user_repo.get_by_name = MagicMock(return_value=None)
        with pytest.raises(BadRequestError, match="reserved"):
            rbac_service.create_group("admin")

    def test_user_name_clashes_with_group(self, rbac_service):
        rbac_service._group_repo.get_by_name = MagicMock(return_value=fake_group(name="taken"))
        user = ProcessedUser(email="t@k.app", name="taken", password_hash="h")
        with pytest.raises(BadRequestError, match="group named"):
            rbac_service.create_user(user)

    def test_group_name_clashes_with_user(self, rbac_service):
        rbac_service._user_repo.get_by_name = MagicMock(return_value=fake_user(name="taken"))
        with pytest.raises(BadRequestError, match="user named"):
            rbac_service.create_group("taken")


# ==================================================================================================
# Patch user extra fields
# ==================================================================================================


class TestPatchUserFields:
    def test_patch_user_full_name(self, rbac_service):
        db_user = fake_user(email="u@k.app", name="u")
        db_user.full_name = None
        db_user.external_id = None
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        user = ProcessedUser(email="u@k.app", full_name="Full Name", password_hash="h")

        out = rbac_service.patch_user(user)
        assert out.full_name == "Full Name"
        assert db_user.full_name == "Full Name"

    def test_patch_user_external_id(self, rbac_service):
        db_user = fake_user(email="u@k.app", name="u")
        db_user.full_name = None
        db_user.external_id = None
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        user = ProcessedUser(email="u@k.app", external_id="orcid-123", password_hash="h")

        out = rbac_service.patch_user(user)
        assert out.orcid == "orcid-123"
        assert db_user.external_id == "orcid-123"


# ==================================================================================================
# Delete user not found
# ==================================================================================================


class TestDeleteUserNotFound:
    def test_remove_user_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_email = MagicMock(return_value=None)
        user = ProcessedUser(email="noone@k.app", name="x_user", password_hash="h")
        with pytest.raises(UnauthorizedError, match="doesn't exists"):
            rbac_service.remove_user(user)

    def test_remove_user_by_id_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(UnauthorizedError, match="doesn't exists"):
            rbac_service.remove_user_by_id(uuid4())


# ==================================================================================================
# _ensure_subject_by_id edge cases
# ==================================================================================================
