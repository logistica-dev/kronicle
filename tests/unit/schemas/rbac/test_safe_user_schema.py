# tests/unit/schemas/rbac/test_safe_user_schema.py
from uuid import uuid4

import pytest

from kronicle.auth.pwd.pwd_manager import PasswordManager
from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.errors.error_types import BadRequestError
from kronicle.schemas.rbac.input_user_schemas import InputUser
from kronicle.schemas.rbac.safe_user_schemas import OutputUser, ProcessedUser
from kronicle.utils.singleton import Singleton


# --- Mock PasswordManager for testing ---
class MockPasswordManager:
    def validate_password(self, pwd: str):
        if len(pwd) < 6:
            raise BadRequestError("Too short")
        return True

    def hash_password(self, pwd: str) -> str:
        return f"hashed-{pwd}"


# Inject a fake PasswordManager into the singleton registry
@pytest.fixture(autouse=True)
def patch_pwd_manager(monkeypatch):
    monkeypatch.setitem(Singleton._instances, PasswordManager, MockPasswordManager())


# --- ProcessedUser tests ---
def test_processed_user_from_input():
    input_user = InputUser(email="test@example.com", password="1valid!Pwd", name="UserName")
    proc_user = ProcessedUser.from_input(input_user)
    assert proc_user.password_hash
    assert proc_user.email == input_user.email
    assert proc_user.name == input_user.name


# --- OutputUser tests ---
def test_output_user_from_db_user_superuser():
    db_user = RbacUser(
        id=uuid4(),
        email="db@example.com",
        name="DbUser",
        external_id="ORCID123",
        full_name="Full Name",
        password_hash="hashed-pwd",
        is_active=True,
        is_superuser=True,
        details={"auth_method": "local"},
    )
    out_user = OutputUser.from_db(db_user)
    assert out_user.is_su is True
    assert out_user.model_dump()["is_su"] is True


def test_output_user_from_db_user_normal():
    db_user = RbacUser(
        id=uuid4(),
        email="db@example.com",
        name="DbUser",
        external_id="ORCID123",
        full_name="Full Name",
        password_hash="hashed-pwd",
        is_active=True,
        is_superuser=False,
        details={"auth_method": "local"},
    )
    out_user = OutputUser.from_db(db_user)
    assert out_user.is_su is False
    assert "is_su" not in out_user.model_dump()


# --- Direct validator calls & OutputUser serialization ---
def test_processed_user_validate_name_none_returns_none():
    assert ProcessedUser.validate_user_name_syntax(None) is None  # type: ignore[call-arg]  # pydantic wraps cls


def test_processed_user_validate_name_invalid_raises():
    with pytest.raises(BadRequestError):
        ProcessedUser.validate_user_name_syntax("1abc")  # type: ignore[call-arg]  # pydantic wraps cls


def test_output_user_model_dump_json():
    out = OutputUser(id=uuid4(), email="a@example.com", name="Alice")
    js = out.model_dump_json()
    assert isinstance(js, str)
    assert "Alice" in js


def test_output_user_str():
    out = OutputUser(id=uuid4(), email="a@example.com", name="Alice")
    assert str(out).startswith("OutUser ")
    assert "Alice" in str(out)
