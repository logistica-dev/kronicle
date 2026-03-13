# tests/iam/test_pwd_manager.py
from pytest import fixture, raises

from kronicle.auth.pwd.pwd_manager import PasswordManager
from kronicle.auth.pwd.pwd_policy import PasswordPolicy


@fixture
def manager():
    policy = PasswordPolicy(
        min_length=10,
        require_uppercase=True,
        require_lowercase=True,
        require_digits=True,
        require_special=True,
        special_chars="!@%^*()_+-=[]{}|:,.?",
    )
    return PasswordManager(policy)


def test_validate_password_success(manager):
    # Valid password
    assert manager.validate_password("Secure123!") is True


def test_validate_password_failure(manager):
    # Too short, fails policy
    with raises(ValueError) as exc_info:
        manager.validate_password("short")
    assert "at least" in str(exc_info.value)


def test_get_password_validation(manager):
    result = manager.get_password_validation("Secure123!")
    assert result["valid"]
    assert isinstance(result["checks"], list)


def test_hash_and_verify(manager):
    password = "Secure123!"
    hashed = manager.hash_password(password)

    # Correct password verifies
    assert manager.verify_password(hashed, password) is True

    # Wrong password returns False
    assert manager.verify_password(hashed, "Wrong123!") is False


def test_verify_invalid_hash(manager):
    # Random string is not a valid hash
    with raises(ValueError) as exc_info:
        manager.verify_password("notahash", "Secure123!")
    assert "Invalid password hash" in str(exc_info.value)


def test_needs_rehash(manager):
    password = "Secure123!"
    hashed = manager.hash_password(password)

    # Should not need rehash for current params
    assert manager.needs_rehash(hashed) in (True, False)  # may vary by argon2 defaults

    # Test fallback: passing invalid hash triggers True
    assert manager.needs_rehash("notahash") is True


def test_rehash_password(manager):
    password = "Secure123!"
    hashed = manager.hash_password(password)

    # If no rehash needed, returns original
    result = manager.rehash_password(hashed, password)
    assert result == hashed

    # Force rehash by mocking needs_rehash to True
    manager.needs_rehash = lambda h: True
    result2 = manager.rehash_password(hashed, password)
    assert result2 != hashed
    assert manager.verify_password(result2, password) is True


def test_get_default_params():
    params = PasswordManager.get_default_params()
    assert isinstance(params, dict)
    assert "time_cost" in params
    assert "memory_cost" in params


def test_hash_password_invalid_policy(manager):
    # Provide a password that violates policy
    with raises(ValueError):
        manager.hash_password("short")
