# kronicle/auth/pwd/pwd_manager.py
import logging
from typing import Optional

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHash, VerificationError, VerifyMismatchError

from kronicle.auth.pwd.pwd_policy import PasswordPolicy, ValidationResult
from kronicle.errors.error_types import BadRequestError

# Default Argon2id parameters (adjust based on your security requirements)
_DEFAULT_PARAMS = {
    "time_cost": 3,
    "memory_cost": 65536,  # 64MB
    "parallelism": 4,
    "hash_len": 32,
    "salt_len": 16,
}


class PasswordManager:
    """
    Secure password management using Argon2id.
    Handles password hashing, verification, and rehashing.
    Implements a class instance approach (stateless, configured once).
    """

    _instance: Optional["PasswordManager"] = None

    def __new__(cls, *args, **kwargs) -> "PasswordManager":
        # If singleton already exists, return it
        if cls._instance is not None:
            return cls._instance
        # Otherwise, create a new instance (will be initialized via init_async)
        return super().__new__(cls)

    def __init__(self, policy: PasswordPolicy | None = None, **argon_params):
        """
        Initialize with Argon2id parameters and password policy.

        Args:
            argon_params: Dictionary of Argon2 parameters (time_cost, memory_cost, etc.)
                       Defaults to _DEFAULT_PARAMS if not provided
        """
        self.policy = policy or PasswordPolicy()
        self.pwd_hasher = PasswordHasher(**{**_DEFAULT_PARAMS, **argon_params})
        self.logger = logging.getLogger(__name__)

    @classmethod
    def initialize(cls, policy, **argon_params) -> "PasswordManager":
        if cls._instance is not None:
            raise RuntimeError("PasswordManager already initialized")
        cls._instance = cls(policy, **argon_params)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "PasswordManager":
        if cls._instance is None:
            raise RuntimeError("Not initialized. Call PasswordManager.initialize() first")
        return cls._instance

    def validate_password(self, password: str) -> bool:
        """Check if password meets minimum requirements."""
        result: ValidationResult = self.policy.check(password)
        if not result["valid"] and result["errors"]:
            raise BadRequestError("; ".join(result["errors"]))
        return result["valid"]

    def get_password_validation(self, password: str) -> ValidationResult:
        """Check if password meets minimum requirements."""
        return self.policy.check(password)

    def hash_password(self, password: str) -> str:
        """
        Hash a password using Argon2id.

        Args:
            password: Plaintext password

        Returns:
            Hashed password string

        Raises:
            RuntimeError: If password doesn't meet requirements
        """
        self.validate_password(password)

        try:
            return self.pwd_hasher.hash(password)
        except Exception as e:  # pragma: no cover
            self.logger.error(f"Password hashing failed: {e}")
            raise RuntimeError("Password hashing failed") from e

    def verify_password(self, hashed_password: str, input_password: str) -> bool:
        """
        Verify a password against its hash.

        Args:
            hashed_password: Stored hash
            input_password: Password to verify

        Returns:
            True if password matches, False otherwise

        Raises:
            BadRequestError: If hash is invalid
        """
        try:
            return self.pwd_hasher.verify(hashed_password, input_password)
        except VerifyMismatchError:
            self.logger.warning("Password verification failed (mismatch)")
            return False
        except (VerificationError, InvalidHash) as e:
            self.logger.error(f"Password verification error: {e}")
            raise BadRequestError("Invalid password") from e

    def needs_rehash(self, hashed_password: str) -> bool:
        """
        Check if a password needs rehashing with current parameters.

        Args:
            hashed_password: Existing hash

        Returns:
            True if rehash needed, False otherwise
        """
        try:
            return self.pwd_hasher.check_needs_rehash(hashed_password)
        except Exception:
            return True  # If we can't check, assume we need to rehash

    def rehash_password(self, hashed_password: str, password: str) -> str:
        """
        Rehash a password if needed.

        Args:
            hashed_password: Existing hash
            password: Plaintext password

        Returns:
            New hash (or original if no rehash needed)
        """
        if self.needs_rehash(hashed_password):
            return self.hash_password(password)
        return hashed_password

    @classmethod
    def get_default_params(cls) -> dict[str, int]:
        """Get default Argon2id parameters."""
        return _DEFAULT_PARAMS.copy()
