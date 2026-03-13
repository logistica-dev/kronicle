# kronicle/auth/pwd/pwd_policy.py
import re
from typing import ClassVar, TypedDict

from pydantic import BaseModel, ConfigDict


class CheckResult(TypedDict):
    name: str
    valid: bool
    weight: float
    error: str | None


class ValidationResult(TypedDict):
    valid: bool
    errors: list[str] | None
    strength: float
    checks: list[CheckResult]


class PasswordCheck(BaseModel):
    """Base class for individual password checks."""

    name: str
    weight: float = 0.1
    required: bool = True

    def check(self, password: str) -> tuple[bool, str | None]:
        """Validate the password against this check."""
        raise NotImplementedError  # pragma: no cover


class LengthCheck(PasswordCheck):
    """Check password length requirements."""

    min_length: int
    max_length: int = 64

    def check(self, password: str) -> tuple[bool, str | None]:
        if len(password) < self.min_length:
            return False, f"Must be at least {self.min_length} characters"
        if len(password) > self.max_length:
            return False, f"Cannot exceed {self.max_length} characters"
        return True, None


class CharacterTypeCheck(PasswordCheck):
    """Check for required character types."""

    name: str = "characters"

    pattern: str
    error_message: str

    def check(self, password: str) -> tuple[bool, str | None]:
        if not re.search(self.pattern, password):
            return False, self.error_message
        return True, None


class CommonPasswordCheck(PasswordCheck):
    """Check against common passwords."""

    common_passwords: ClassVar[set[str]] = {
        "password",
        "motdepasse",
        "123456",
        "qwerty",
        "letmein",
        "welcome",
        "admin",
        "monkey",
        "sunshine",
        "password1",
        "123123",
    }

    def check(self, password: str) -> tuple[bool, str | None]:
        if password.lower() in self.common_passwords:
            return False, "Password is too common"
        return True, None


class SequentialCheck(PasswordCheck):
    """Check for sequential/repeated characters."""

    def check(self, password: str) -> tuple[bool, str | None]:
        if re.search(r"(.)\1{3,}", password):
            return False, "Contains too many repeated characters"
        return True, None


class KeyboardPatternCheck(PasswordCheck):
    """Check for keyboard patterns."""

    patterns: ClassVar[list[str]] = [
        "qwerty",
        "asdfgh",
        "zxcvbn",
        "123456",
        "654321",
        "!@#$%^",
        "1qaz",
        "2wsx",
        "3edc",
        "4rfv",
        "5tgb",
    ]

    def check(self, password: str) -> tuple[bool, str | None]:
        password_lower = password.lower()
        for pattern in self.patterns:
            if pattern in password_lower:
                return False, "Contains common keyboard patterns"
        return True, None


class ForbiddenCharactersCheck(PasswordCheck):
    """Check for forbidden characters in passwords."""

    forbidden_chars: ClassVar[str] = r'[\'"\\$`;&|<>#\n\r\t]'
    forbidden_chars_readable: ClassVar[str] = "'\"\\$`;&|<># (and whitespace)"

    def check(self, password: str) -> tuple[bool, str | None]:
        if re.search(self.forbidden_chars, password):
            return False, f"Cannot contain forbidden characters: {self.forbidden_chars_readable}"
        return True, None


class PasswordPolicy(BaseModel):
    """Password policy configuration and validation."""

    model_config = ConfigDict(frozen=True)

    min_length: int = 10
    max_length: int = 128
    require_uppercase: bool = True
    require_lowercase: bool = True
    require_digits: bool = True
    require_special: bool = True
    special_chars: str = "!@%^*()_+-=[]{}|:,.?"

    @property
    def checks(self) -> list[PasswordCheck]:
        """Generate the list of checks based on policy settings."""
        checks = [
            LengthCheck(name="length", min_length=self.min_length),
            ForbiddenCharactersCheck(name="forbidden_chars"),
            (
                CharacterTypeCheck(
                    name="uppercase", pattern=r"[A-Z]", error_message="Must contain at least one uppercase letter"
                )
                if self.require_uppercase
                else None
            ),
            (
                CharacterTypeCheck(
                    name="lowercase", pattern=r"[a-z]", error_message="Must contain at least one lowercase letter"
                )
                if self.require_lowercase
                else None
            ),
            (
                CharacterTypeCheck(name="digits", pattern=r"[0-9]", error_message="Must contain at least one digit")
                if self.require_digits
                else None
            ),
            (
                CharacterTypeCheck(
                    name="special",
                    pattern=f"[{re.escape(self.special_chars)}]",
                    error_message=f"Must contain at least one special character: {self.special_chars}",
                )
                if self.require_special
                else None
            ),
            CommonPasswordCheck(name="common"),
            SequentialCheck(name="sequential"),
            KeyboardPatternCheck(name="keyboard_patterns"),
        ]

        return checks

    def check(self, password: str) -> ValidationResult:
        """
        Validate password against all checks.

        Returns:
            dict: {
                'valid': bool,
                'errors': list[str] | None,
                'strength': float (0-1),
                'checks': list[dict[str, Any]]  # Detailed check results
            }
        """
        if not password:
            return ValidationResult(
                valid=False,
                errors=["Password cannot be empty"],
                strength=0.0,
                checks=[],
            )

        errors = []
        strength = 0.0
        check_results = []

        for check in self.checks:
            valid, error = check.check(password)
            check_result = CheckResult(name=check.name, valid=valid, weight=check.weight, error=error)
            check_results.append(check_result)

            if not valid and error:
                errors.append(error)
            else:
                strength += check.weight

        # Calculate entropy-based strength
        unique_chars = len(set(password))
        strength += min(0.3, unique_chars / 30)  # Max 0.3 for 30+ unique chars

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors if errors else [],
            strength=min(1.0, strength),
            checks=check_results,
        )
