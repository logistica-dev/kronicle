# kronicle/schemas/rbac/user_schemas.py
from __future__ import annotations

from re import fullmatch
from typing import Any
from uuid import UUID

from pydantic import BaseModel, EmailStr, PrivateAttr, field_validator

from kronicle.auth.pwd.pwd_manager import PasswordManager
from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.errors.error_types import BadRequestError
from kronicle.schemas.rbac.input_user_schemas import InputUser

pwd_manager = PasswordManager()

# Allowed characters after the first letter
_ALLOWED_CHARS = "A-Za-z0-9_ .@-"
_USERNAME_MIN_LENGTH = 4
_USERNAME_MAX_LENGTH = 64
# Regex: first char is letter, rest are from ALLOWED_CHARS, total length 4–64
_USERNAME_REGEX = rf"[A-Za-z][{_ALLOWED_CHARS}]{{{_USERNAME_MIN_LENGTH - 1},{_USERNAME_MAX_LENGTH - 1}}}"

mod = "outusr"


class ProcessedUser(BaseModel):
    """
    Represents validated and processed user data ready for database insertion.
    Contains derived fields (e.g., hashed passwords) and metadata.
    """

    email: EmailStr
    password_hash: str | None = None  # Hashed password (never store raw passwords!)
    name: str | None = None
    orcid: str | None = None
    full_name: str | None = None
    details: dict[str, Any] = {"auth_method": "local"}  # Default metadata
    group_name: str | None = None
    zone_name: str | None = None

    @field_validator("name")
    def validate_username_syntax(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if not fullmatch(_USERNAME_REGEX, v):
            raise BadRequestError(
                "Username must start with a letter, be 4–64 characters long, "
                "and only contain letters, digits, '_', '.', '-', '@', or space"
            )
        return v

    @classmethod
    def from_input(cls, data: InputUser):
        hashed = pwd_manager.hash_password(data.password) if data.password else None
        return ProcessedUser(
            email=data.email,
            password_hash=hashed,
            name=data.name,
            full_name=data.full_name,
            orcid=data.orcid,
            details={"auth_method": "local"},  # explicitly derived
        )

    def to_db_user(self) -> RbacUser:
        """Convert this processed user data into a RbacUser for persistence."""
        return RbacUser(
            email=self.email,
            name=self.name,
            password_hash=self.password_hash,
            external_id=self.orcid,
            full_name=self.full_name,
            is_active=True,
            is_superuser=False,
            details=self.details,
        )


class OutputUser(BaseModel):
    """
    Represents validated and processed user data ready for database insertion.
    Contains derived fields (e.g., hashed passwords) and metadata.
    """

    id: UUID
    email: EmailStr
    name: str | None = None
    orcid: str | None = None
    full_name: str | None = None
    details: dict[str, Any] = {"auth_method": "local"}  # Default metadata

    # Internal attribute, not part of .dict()/JSON by default
    _is_su: bool = PrivateAttr(False)

    @property
    def is_su(self):
        return self._is_su

    def _set_su(self):
        self._is_su = True

    @classmethod
    def from_db_user(cls, db_user: RbacUser) -> OutputUser:
        """Convert this processed user data into a RbacUser for persistence."""
        # here = "from_db_user"
        # log_d(here, "db_user", db_user)
        # log_d(here, "db_user.is_superuser", db_user.is_superuser)
        usr = cls(
            id=db_user.id,
            email=db_user.email,
            name=db_user.name,
            orcid=db_user.external_id,
            full_name=db_user.full_name,
            details=db_user.details,
        )
        if db_user.is_superuser:
            usr._set_su()
        # log_d(here, "usr.is_superuser", usr.is_su)
        return usr

    # Include is_su in dict/json output
    def model_dump(self, *args, **kwargs):
        d = super().model_dump(*args, **kwargs)
        if self._is_su:
            d["is_su"] = True
        return d

    # Include is_su in JSON output
    def model_dump_json(self, *args, **kwargs):
        return super().model_dump_json(*args, **kwargs, **{"include": {"is_su"} if self._is_su else {}})
