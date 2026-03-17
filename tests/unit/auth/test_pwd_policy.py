# tests/test_password_policy.py
from __future__ import annotations

import unittest

from kronicle.auth.pwd.pwd_policy import (
    CharacterTypeCheck,
    CommonPasswordCheck,
    ForbiddenCharactersCheck,
    KeyboardPatternCheck,
    LengthCheck,
    PasswordCheck,
    PasswordPolicy,
    SequentialCheck,
)


class TestPasswordChecks(unittest.TestCase):
    """Test individual password check classes."""

    def test_length_check(self):
        check = LengthCheck(name="length", min_length=8, max_length=64)

        # Test valid lengths
        self.assertEqual(check.check("12345678"), (True, None))  # Exactly min length
        self.assertEqual(check.check("a" * 32), (True, None))  # Middle length
        self.assertEqual(check.check("a" * 64), (True, None))  # Max length

        # Test invalid lengths
        is_pwd_valid, err_msg = check.check("short")
        self.assertFalse(is_pwd_valid)
        self.assertIsNotNone(err_msg)
        self.assertIn("at least 8 characters", err_msg)  # type: ignore

        is_pwd_valid, err_msg = check.check("a" * 65)
        self.assertFalse(is_pwd_valid)
        self.assertIn("Cannot exceed 64 characters", err_msg)  # type: ignore

    def test_character_type_check(self):
        # Test uppercase check
        validation = CharacterTypeCheck(name="uppercase", pattern=r"[A-Z]", error_message="Must contain uppercase")
        self.assertEqual(validation.check("password"), (False, "Must contain uppercase"))
        self.assertEqual(validation.check("Password"), (True, None))

    def test_common_password_check(self):
        validation = CommonPasswordCheck(name="common")
        self.assertEqual(validation.check("password"), (False, "Password is too common"))
        self.assertEqual(validation.check("Secure123!"), (True, None))

    def test_sequential_check(self):
        validation = SequentialCheck(name="sequential")
        self.assertEqual(validation.check("aaaa"), (False, "Contains too many repeated characters"))
        self.assertEqual(validation.check("abcde"), (True, None))

    def test_keyboard_pattern_check(self):
        validation = KeyboardPatternCheck(name="keyboard_patterns")
        self.assertEqual(validation.check("qwerty"), (False, "Contains common keyboard patterns"))
        self.assertEqual(validation.check("secure123"), (True, None))

    def test_forbidden_characters_check(self):
        validation = ForbiddenCharactersCheck(name="forbidden_chars")
        forbidden_msg = "Cannot contain forbidden characters: '\"\\$`;&|<># (and whitespace)"
        self.assertEqual(validation.check('password"'), (False, forbidden_msg))
        self.assertEqual(validation.check("password'"), (False, forbidden_msg))
        self.assertEqual(validation.check("password\\"), (False, forbidden_msg))
        self.assertEqual(validation.check("secure123!"), (True, None))


class TestPasswordPolicy(unittest.TestCase):
    """Test the complete password policy."""

    def setUp(self):
        self.policy = PasswordPolicy(
            min_length=10,
            require_uppercase=True,
            require_lowercase=True,
            require_digits=True,
            require_special=True,
            special_chars="!@%^*()_+-=[]{}|:,.?",
        )

    def test_policy_no_param(self):
        validation = self.policy.check("")
        self.assertFalse(validation["valid"])
        assert any("cannot be empty" in err for err in validation["errors"])  # type: ignore

    def test_policy_validation(self):
        # Test valid password
        validation = self.policy.check("Secure123!")
        self.assertTrue(validation["valid"])
        self.assertGreater(validation["strength"], 0.8)

        # Test invalid passwords
        # Too short
        validation = self.policy.check("Short1!")
        self.assertFalse(validation["valid"])
        assert any("at least 10 characters" in err for err in validation["errors"])  # type: ignore

        # Missing uppercase
        validation = self.policy.check("secure123!")
        self.assertFalse(validation["valid"])
        assert any("uppercase letter" in err for err in validation["errors"])  # type: ignore

        # Missing special character
        validation = self.policy.check("Secure123")
        self.assertFalse(validation["valid"])
        assert any("special character" in err for err in validation["errors"])  # type: ignore

        # With forbidden character
        validation = self.policy.check('Secure123"')
        self.assertFalse(validation["valid"])
        assert any("forbidden characters" in err for err in validation["errors"])  # type: ignore

    def test_policy_checks_property(self):
        checks = self.policy.checks
        self.assertGreater(len(checks), 0)
        self.assertIsInstance(checks[0], PasswordCheck)

    def test_policy_checks_skipped(self):
        policy = PasswordPolicy(
            require_uppercase=False, require_lowercase=False, require_digits=False, require_special=False
        )
        checks = policy.checks
        # Should include only LengthCheck, ForbiddenCharactersCheck, CommonPasswordCheck,
        #   SequentialCheck, KeyboardPatternCheck
        names = [c.name for c in checks if c is not None]
        assert "uppercase" not in names
        assert "lowercase" not in names
        assert "digits" not in names
        assert "special" not in names


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
