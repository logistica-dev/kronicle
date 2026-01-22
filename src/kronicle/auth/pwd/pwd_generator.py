# kronicle/auth/pwd/pwd_generator.py
import random
import string

from kronicle.auth.pwd.pwd_manager import PasswordPolicy


class PasswordGenerator:
    """
    Secure password generator that complies with a given PasswordPolicy.
    Generates passwords that meet all policy requirements.
    """

    # Basic character sets
    LOWERCASE = string.ascii_lowercase
    UPPERCASE = string.ascii_uppercase
    DIGITS = string.digits

    def __init__(self, policy: PasswordPolicy):
        """
        Initialize the password generator with a specific password policy.

        Args:
            policy: PasswordPolicy instance defining requirements
        """
        self.policy = policy
        self.min_length = max(12, policy.min_length)  # Enforce minimum security

        # Build allowed special characters from policy
        self.ALLOWED_SPECIAL = policy.special_chars

        # Build complete allowed character set
        self._allowed_chars = self.LOWERCASE
        if policy.require_uppercase:
            self._allowed_chars += self.UPPERCASE
        if policy.require_digits:
            self._allowed_chars += self.DIGITS
        if policy.require_special:
            self._allowed_chars += self.ALLOWED_SPECIAL

    def _get_required_chars(self) -> list[str]:
        """Get one character from each required character set."""
        chars = []

        # Always include lowercase (basic requirement)
        chars.append(random.choice(self.LOWERCASE))

        if self.policy.require_uppercase:
            chars.append(random.choice(self.UPPERCASE))
        if self.policy.require_digits:
            chars.append(random.choice(self.DIGITS))
        if self.policy.require_special:
            chars.append(random.choice(self.ALLOWED_SPECIAL))

        return chars

    def generate(self, length: int | None = None) -> str:
        """
        Generate a secure password that complies with the policy.

        Args:
            length: Desired password length. If None, uses a random length
                    between min_length and min_length+8.

        Returns:
            str: Generated password that meets all policy requirements
        """
        # Get required characters from each set
        required_chars = self._get_required_chars()
        nb_required_chars = len(required_chars)

        # Determine password length
        if length is None:
            length = random.randint(self.min_length, self.min_length + 8)
        else:
            length = max(self.min_length, length, nb_required_chars)

        # Generate remaining characters from all allowed sets
        remaining_length = length - nb_required_chars
        if remaining_length < 0:
            raise ValueError(f"Password length must be at least {nb_required_chars}")  # pragma: no cover

        remaining_chars = [random.choice(self._allowed_chars) for _ in range(remaining_length)]

        # Combine and shuffle all characters
        all_chars = required_chars + remaining_chars
        random.shuffle(all_chars)

        # Convert to string and validate
        password = "".join(all_chars)

        # Double-check the password meets requirements
        check = self.policy.check(password)
        if not check["valid"]:
            # This should theoretically never happen, but just in case
            return self.generate(length)  # Try again

        return password

    def generate_multiple(self, count: int = 5, length: int | None = None) -> list[str]:
        """
        Generate multiple passwords that comply with the policy.

        Args:
            count: Number of passwords to generate
            length: Fixed length for all passwords

        Returns:
            list[str]: List of generated passwords
        """
        return [self.generate(length) for _ in range(count)]
