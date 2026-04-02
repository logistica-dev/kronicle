# tests/unit/schemas/rbac/test_user_schema.py
from uuid import uuid4

import pytest

from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.schemas.rbac.input_user_schemas import InputUser
from kronicle.schemas.rbac.safe_user_schemas import OutputUser, ProcessedUser


# --- Mock PasswordManager for testing ---
class MockPasswordManager:
    def validate_password(self, pwd: str):
        if len(pwd) < 6:
            raise ValueError("Too short")
        return True

    def hash_password(self, pwd: str) -> str:
        return f"hashed-{pwd}"


# Patch the PasswordManager in the module
@pytest.fixture(autouse=True)
def patch_pwd_manager(monkeypatch):
    monkeypatch.setattr("kronicle.auth.pwd.pwd_manager.PasswordManager", lambda: MockPasswordManager())


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
    out_user = OutputUser.from_db_user(db_user)
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
    out_user = OutputUser.from_db_user(db_user)
    assert out_user.is_su is False
    assert "is_su" not in out_user.model_dump()
