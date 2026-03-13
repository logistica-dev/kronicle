# tests/iam/test_pwd_generator.py
import re

import pytest

from kronicle.auth.pwd.pwd_generator import PasswordGenerator
from kronicle.auth.pwd.pwd_manager import PasswordPolicy

# -----------------------
# PasswordGenerator tests
# -----------------------


@pytest.fixture
def generator():
    policy = PasswordPolicy(
        min_length=12,
        require_uppercase=True,
        require_lowercase=True,
        require_digits=True,
        require_special=True,
        special_chars="!@%*_+-=|:,.?",
    )
    return PasswordGenerator(policy)


# def test_generate_length_too_short_raises():
#     # Policy requires lowercase + uppercase + digit + special = 4 required chars
#     policy = PasswordPolicy(
#         min_length=1,  # force generator to allow small min_length
#         require_uppercase=True,
#         require_lowercase=True,
#         require_digits=True,
#         require_special=True,
#         special_chars="!@#",
#     )
#     generator = PasswordGenerator(policy)

#     # Attempt to generate a password shorter than 4 characters
#     with pytest.raises(ValueError) as exc_info:
#         generator.generate(length=3)  # less than required 4 chars
#     assert "Password length must be at least" in str(exc_info.value)


def test_generate_single_password(generator):
    password = generator.generate()
    assert len(password) >= 12

    result = generator.policy.check(password)
    assert result["valid"]


def test_generate_password_with_length(generator):
    password = generator.generate(length=16)
    assert len(password) == 16
    result = generator.policy.check(password)
    assert result["valid"]


def test_generate_multiple_passwords(generator):
    passwords = generator.generate_multiple(count=5, length=20)
    assert len(passwords) == 5

    for pwd in passwords:
        assert len(pwd) == 20
        result = generator.policy.check(pwd)
        assert result["valid"]


def test_generated_password_contains_required_characters(generator):
    for _ in range(10):
        pwd = generator.generate()
        assert any(c.isupper() for c in pwd), "Missing uppercase character"
        assert any(c.islower() for c in pwd), "Missing lowercase character"
        assert any(c.isdigit() for c in pwd), "Missing digit"
        assert any(c in generator.policy.special_chars for c in pwd), "Missing special character"
        assert not re.search(r'[\'"\\$`;&|<>#\n\r\t]', pwd), "Password contains forbidden characters"


def test_generated_password_strength(generator):
    for _ in range(5):
        pwd = generator.generate()
        result = generator.policy.check(pwd)
        assert result["strength"] > 0.7


if __name__ == "__main__":  # pragma: no cover
    import sys

    import pytest

    # Run pytest on this file only, with verbose output
    sys.exit(pytest.main([__file__, "-v"]))
