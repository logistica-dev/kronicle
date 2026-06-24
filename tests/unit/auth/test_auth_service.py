from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from kronicle.auth.auth_service import AuthService
from kronicle.errors.error_types import NotFoundError, UnauthorizedError


@pytest.fixture
def mock_jwt_service():
    svc = MagicMock()
    svc.create_access_token.return_value = "jwt-token"
    return svc


@pytest.fixture
def mock_rbac_service():
    svc = MagicMock()
    return svc


@pytest.fixture
def mock_pwd_manager():
    mgr = MagicMock()
    mgr.verify_password.return_value = True
    mgr.needs_rehash.return_value = False
    mgr.hash_password.return_value = "new-hashed-password"
    return mgr


@pytest.fixture
def service(mock_jwt_service, mock_rbac_service, mock_pwd_manager):
    with patch("kronicle.auth.auth_service.PasswordManager.get_instance", return_value=mock_pwd_manager):
        svc = AuthService(jwt_service=mock_jwt_service, rbac_service=mock_rbac_service)
    return svc


@pytest.fixture
def mock_login():
    creds = MagicMock()
    creds.login = "testuser"
    creds.password = "ValidPass1!"
    creds.is_email = False
    return creds


def make_db_user(**overrides):
    user = MagicMock()
    user.id = overrides.get("id", uuid4())
    user.email = overrides.get("email", "test@example.com")
    user.name = overrides.get("name", "testuser")
    user.password_hash = overrides.get("password_hash", "hashed-password")
    user.is_active = overrides.get("is_active", True)
    user.is_superuser = overrides.get("is_superuser", False)
    user.external_id = overrides.get("external_id", None)
    user.full_name = overrides.get("full_name", None)
    user.details = overrides.get("details", {})
    return user


# --------------------------------------------------------------------------------------
# __init__
# --------------------------------------------------------------------------------------


class TestInit:
    def test_raises_without_jwt_service(self, mock_rbac_service):
        with pytest.raises(RuntimeError, match="JwtService not initialized"):
            AuthService(jwt_service=None, rbac_service=mock_rbac_service)

    def test_raises_without_rbac_service(self, mock_jwt_service):
        with pytest.raises(RuntimeError, match="RbacService not initialized"):
            AuthService(jwt_service=mock_jwt_service, rbac_service=None)

    def test_initializes_successfully(self, mock_jwt_service, mock_rbac_service, mock_pwd_manager):
        with patch("kronicle.auth.auth_service.PasswordManager.get_instance", return_value=mock_pwd_manager):
            svc = AuthService(jwt_service=mock_jwt_service, rbac_service=mock_rbac_service)
        assert svc._jwt_service is mock_jwt_service
        assert svc._rbac_service is mock_rbac_service
        assert svc._pwd_manager is mock_pwd_manager


# --------------------------------------------------------------------------------------
# login
# --------------------------------------------------------------------------------------


class TestLogin:
    def test_successful_login(self, service, mock_rbac_service, mock_jwt_service, mock_pwd_manager, mock_login):
        db_user = make_db_user()
        mock_rbac_service.fetch_user_for_auth.return_value = db_user
        mock_pwd_manager.verify_password.return_value = True
        mock_pwd_manager.needs_rehash.return_value = False

        token = service.login(mock_login)

        assert token == "jwt-token"
        mock_rbac_service.fetch_user_for_auth.assert_called_once_with(mock_login)
        mock_pwd_manager.verify_password.assert_called_once_with(db_user.password_hash, mock_login.password)
        mock_jwt_service.create_access_token.assert_called_once()

    def test_login_user_not_found_raises_unauthorized(self, service, mock_rbac_service, mock_login):
        mock_rbac_service.fetch_user_for_auth.side_effect = NotFoundError("User not found")

        with pytest.raises(UnauthorizedError, match="Invalid credentials"):
            service.login(mock_login)

    def test_login_user_is_none_raises_unauthorized(self, service, mock_rbac_service, mock_login):
        mock_rbac_service.fetch_user_for_auth.return_value = None

        with pytest.raises(UnauthorizedError, match="Invalid credentials"):
            service.login(mock_login)

    def test_login_no_password_hash_raises_unauthorized(self, service, mock_rbac_service, mock_login):
        db_user = make_db_user(password_hash=None)
        mock_rbac_service.fetch_user_for_auth.return_value = db_user

        with pytest.raises(UnauthorizedError, match="Invalid credentials"):
            service.login(mock_login)

    def test_login_inactive_user_raises_unauthorized(self, service, mock_rbac_service, mock_login):
        db_user = make_db_user(is_active=False)
        mock_rbac_service.fetch_user_for_auth.return_value = db_user

        with pytest.raises(UnauthorizedError, match="Invalid credentials"):
            service.login(mock_login)

    def test_login_wrong_password_raises_unauthorized(self, service, mock_rbac_service, mock_pwd_manager, mock_login):
        db_user = make_db_user()
        mock_rbac_service.fetch_user_for_auth.return_value = db_user
        mock_pwd_manager.verify_password.return_value = False

        with pytest.raises(UnauthorizedError, match="Invalid credentials"):
            service.login(mock_login)

    def test_login_verify_exception_raises_unauthorized(self, service, mock_rbac_service, mock_pwd_manager, mock_login):
        db_user = make_db_user()
        mock_rbac_service.fetch_user_for_auth.return_value = db_user
        mock_pwd_manager.verify_password.side_effect = Exception("verification error")

        with pytest.raises(UnauthorizedError, match="Invalid credentials"):
            service.login(mock_login)

    def test_login_with_rehash(self, service, mock_rbac_service, mock_jwt_service, mock_pwd_manager, mock_login):
        db_user = make_db_user()
        mock_rbac_service.fetch_user_for_auth.return_value = db_user
        mock_pwd_manager.verify_password.return_value = True
        mock_pwd_manager.needs_rehash.return_value = True
        mock_pwd_manager.rehash_password.return_value = "rehashed-password"

        token = service.login(mock_login)

        assert token == "jwt-token"
        mock_pwd_manager.rehash_password.assert_called_once_with(db_user.password_hash, mock_login.password)
        mock_rbac_service.update_password_hash.assert_called_once_with(db_user.id, "rehashed-password")


# --------------------------------------------------------------------------------------
# change_password
# --------------------------------------------------------------------------------------


class TestChangePassword:
    @pytest.fixture
    def mock_change_pwd(self):
        creds = MagicMock()
        creds.login = "testuser"
        creds.password = "OldPass1!"
        creds.new_password = "NewPass2@"
        creds.is_email = False
        return creds

    def test_successful_change(self, service, mock_rbac_service, mock_jwt_service, mock_pwd_manager, mock_change_pwd):
        db_user = make_db_user()
        mock_rbac_service.fetch_user_for_auth.return_value = db_user
        mock_pwd_manager.verify_password.return_value = True
        mock_pwd_manager.hash_password.return_value = "new-hash"

        token = service.change_password(mock_change_pwd)

        assert token == "jwt-token"
        mock_rbac_service.update_password_hash.assert_called_once_with(db_user.id, "new-hash")
        mock_jwt_service.create_access_token.assert_called_once()
        mock_pwd_manager.hash_password.assert_called_once_with(mock_change_pwd.new_password)

    def test_change_user_not_found_raises_unauthorized(self, service, mock_rbac_service, mock_change_pwd):
        mock_rbac_service.fetch_user_for_auth.side_effect = NotFoundError("User not found")

        with pytest.raises(UnauthorizedError, match="Invalid credentials"):
            service.change_password(mock_change_pwd)

    def test_change_no_password_hash_raises_unauthorized(self, service, mock_rbac_service, mock_change_pwd):
        db_user = make_db_user(password_hash=None)
        mock_rbac_service.fetch_user_for_auth.return_value = db_user

        with pytest.raises(UnauthorizedError, match="Invalid credentials"):
            service.change_password(mock_change_pwd)

    def test_change_inactive_user_raises_unauthorized(self, service, mock_rbac_service, mock_change_pwd):
        db_user = make_db_user(is_active=False)
        mock_rbac_service.fetch_user_for_auth.return_value = db_user

        with pytest.raises(UnauthorizedError, match="Invalid credentials"):
            service.change_password(mock_change_pwd)

    def test_change_wrong_password_raises_unauthorized(
        self, service, mock_rbac_service, mock_pwd_manager, mock_change_pwd
    ):
        db_user = make_db_user()
        mock_rbac_service.fetch_user_for_auth.return_value = db_user
        mock_pwd_manager.verify_password.return_value = False

        with pytest.raises(UnauthorizedError, match="Invalid credentials"):
            service.change_password(mock_change_pwd)

    def test_change_verify_exception_raises_unauthorized(
        self, service, mock_rbac_service, mock_pwd_manager, mock_change_pwd
    ):
        db_user = make_db_user()
        mock_rbac_service.fetch_user_for_auth.return_value = db_user
        mock_pwd_manager.verify_password.side_effect = Exception("verification error")

        with pytest.raises(UnauthorizedError, match="Invalid credentials"):
            service.change_password(mock_change_pwd)
