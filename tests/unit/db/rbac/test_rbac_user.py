# tests/unit/db/rbac/test_rbac_user.py
from kronicle.db.rbac.models.rbac_user import RbacUser


def _user(**overrides):
    u = RbacUser(email="test@example.com")
    u.is_active = overrides.get("is_active", True)
    u.is_superuser = overrides.get("is_superuser", False)
    return u


def test_model_dump_active_superuser_keeps_superuser_flag():
    d = _user(is_active=True, is_superuser=True).model_dump()
    assert "is_active" not in d
    assert d["is_superuser"] is True


def test_model_dump_active_non_superuser_drops_both_flags():
    d = _user(is_active=True, is_superuser=False).model_dump()
    assert "is_active" not in d
    assert "is_superuser" not in d


def test_model_dump_inactive_non_superuser_keeps_inactive():
    d = _user(is_active=False, is_superuser=False).model_dump()
    assert d["is_active"] is False
    assert "is_superuser" not in d


def test_model_dump_inactive_superuser_keeps_both_flags():
    d = _user(is_active=False, is_superuser=True).model_dump()
    assert d["is_active"] is False
    assert d["is_superuser"] is True


def test_model_dump_keeps_email():
    d = _user().model_dump()
    assert d["email"] == "test@example.com"


def test_repr():
    u = RbacUser(email="alice@example.com")
    assert repr(u) == "<User alice@example.com>"


def test_snapshot_excludes_password_hash():
    u = _user()
    u.password_hash = "$argon2id$hunter2"
    snap = u.snapshot
    assert "password_hash" not in snap
    assert snap["email"] == "test@example.com"
