# tests/unit/schemas/rbac/test_input_user_schema.py

import pytest
from pydantic import ValidationError

from kronicle.auth.pwd.pwd_manager import PasswordManager
from kronicle.errors.error_types import BadRequestError
from kronicle.schemas.rbac.input_user_schemas import (
    InputUser,
    InputUserChangePwd,
    InputUserLogin,
)
from kronicle.utils.singleton import Singleton


# --- Mock PasswordManager for testing ---
class MockPasswordManager:
    def validate_password(self, pwd: str):
        if len(pwd) < 4:
            raise BadRequestError("Too short")
        return True

    def hash_password(self, pwd: str) -> str:
        return f"hashed-{pwd}"


# Inject a fake PasswordManager into the singleton registry
@pytest.fixture(autouse=True)
def patch_pwd_manager(monkeypatch):
    monkeypatch.setitem(Singleton._instances, PasswordManager, MockPasswordManager())


# --- InputUserLogin tests ---
@pytest.mark.parametrize(
    "login,is_email",
    [
        ("user123", False),
        ("User_123", False),
        ("user.name@domain.com", True),
        ("UPPER@EMAIL.COM", True),
    ],
)
def test_login_valid(login, is_email):
    obj = InputUserLogin(login=login, password="validpass")
    assert obj.login == login.lower() if is_email else login
    assert obj.is_email == is_email


@pytest.mark.parametrize(
    "login",
    [
        "1abc",  # starts with digit
        "invalid*char",  # invalid character
    ],
)
def test_login_chars(login):
    with pytest.raises(BadRequestError):
        InputUserLogin(login=login, password="validpass")


@pytest.mark.parametrize(
    "login",
    [
        "ab",  # too short
        "thisusernameiswaytoolongtobevalidandshouldfailbecauseitexceeds64chars",
    ],
)
def test_login_length(login):
    with pytest.raises(ValidationError):
        InputUserLogin(login=login, password="validpass")


# --- InputUserChangePwd tests ---
def test_password_validation_and_match():
    obj = InputUserChangePwd(
        login="user123", password="CurrentPass_124!", new_password="NewPass1!_!", confirm_password="NewPass1!_!"
    )
    assert obj.new_password == "NewPass1!_!"


def test_password_too_short():
    with pytest.raises(BadRequestError):
        InputUserChangePwd(login="user123", password="CurrentPass_124!_!", new_password="123", confirm_password="123")


def test_password_mismatch():
    with pytest.raises(BadRequestError):
        InputUserChangePwd(
            login="user123", password="CurrentPass_124!_!", new_password="newpass1", confirm_password="different"
        )


# --- InputUser tests ---
def test_input_user_valid():
    obj = InputUser(email="test@example.com", password="Valid!Pwd01", name="UserName")
    assert obj.email == "test@example.com"
    assert obj.name == "UserName"


def test_input_user_invalid_email():
    with pytest.raises(ValidationError):
        InputUser(email="not-an-email", password="Valid!Pwd01")


def test_input_user_invalid_password():
    with pytest.raises(BadRequestError):
        InputUser(email="test@example.com", password="123")


def test_input_user_invalid_name():
    with pytest.raises(BadRequestError):
        InputUser(email="test@example.com", password="Valid!Pwd01", name="1abc")


# --- Direct validator calls (hit branches unreachable through construction) ---
def test_validate_login_none_raises():
    from unittest.mock import MagicMock

    with pytest.raises(BadRequestError, match="Input login must be provided"):
        InputUserLogin.validate_login(None, MagicMock())


def test_validate_user_name_syntax_none_returns_none():
    assert InputUser.validate_user_name_syntax(None) is None  # type: ignore


def test_validate_user_name_syntax_invalid_raises():
    with pytest.raises(BadRequestError):
        InputUser.validate_user_name_syntax("1abc")  # type: ignore


def test_input_user_patch_falsy_name_returns_none():
    from kronicle.schemas.rbac.input_user_schemas import InputUserPatch

    patch = InputUserPatch(name=None, full_name=None)
    assert patch.name is None
    assert patch.full_name is None


def test_input_user_patch_invalid_name_raises():
    from kronicle.schemas.rbac.input_user_schemas import InputUserPatch

    with pytest.raises(BadRequestError):
        InputUserPatch(name="1abc")
