# kronicle/schemas/rbac/input_user_schemas.py
from __future__ import annotations

from typing import Annotated

from email_validator import validate_email
from pydantic import BaseModel, EmailStr, Field, field_validator, model_validator

from kronicle.auth.pwd.pwd_manager import PasswordManager
from kronicle.errors.error_types import BadRequestError
from kronicle.utils.str_utils import validate_name_syntax

# Username: allowed characters after the first letter
_USERNAME_MIN_LENGTH = 4
_USERNAME_MAX_LENGTH = 64
_USERNAME_EXTRA_CHARS = "_ .@-"

mod = "inuser"


class InputUserLogin(BaseModel):
    login: Annotated[
        str,
        Field(
            min_length=_USERNAME_MIN_LENGTH,
            max_length=_USERNAME_MAX_LENGTH,
            description="Username or email used to login",
        ),
    ]
    password: Annotated[str, Field(description="User password (validated against server password policy)")]
    _is_email: bool = False  # Filled automatically by validator

    @property
    def is_email(self):
        return self._is_email

    @field_validator("login")
    @classmethod
    def validate_login(cls, v: str | None, info) -> str:
        if v is None:
            raise BadRequestError("Input login must be provided")
        try:
            validate_email(v)
            # Mark that this is an email
            info.data["_is_email"] = True
            return v.lower()
        except Exception:
            pass
        try:
            validate_name_syntax(v, extra_chars="_ .@-", min_length=4, max_length=64)
        except ValueError as e:
            raise BadRequestError(f"User {e}") from e
        info.data["_is_email"] = False
        return v


class InputUserChangePwd(InputUserLogin):
    new_password: Annotated[str, Field(description="New password")]
    confirm_password: Annotated[str, Field(description="Confirm new password")]

    @field_validator("new_password")
    def validate_new_password(cls, v: str) -> str:
        try:
            PasswordManager().validate_password(v)
        except ValueError as e:
            raise BadRequestError(f"Invalid password: {e}") from e
        return v

    @model_validator(mode="after")
    def check_password_match(self):
        if self.new_password != self.confirm_password:
            raise BadRequestError("New passwords do not match")
        return self


class InputUser(BaseModel):
    """
    Represents raw input data for user creation.
    Validates and sanitizes user-provided data before processing.
    May reject invalid data (e.g., weak passwords, invalid emails).
    """

    email: EmailStr  # Validates email format
    password: str | None = None  # Raw password (will be hashed later)
    orcid: str | None = None
    name: str | None = None  # optional for now
    full_name: str | None = None

    @field_validator("password")
    def validate_password_syntax(cls, v: str) -> str:
        """Validate raw password if present for local users."""
        try:
            PasswordManager().validate_password(v)  # password validation takes place there.
        except ValueError as e:
            raise BadRequestError(f"Invalid password: {e}") from e
        return v

    @field_validator("name", "full_name")
    def validate_user_name_syntax(cls, v: str | None) -> str | None:
        try:
            return validate_name_syntax(
                v, extra_chars=_USERNAME_EXTRA_CHARS, min_length=_USERNAME_MIN_LENGTH, max_length=_USERNAME_MAX_LENGTH
            )
        except ValueError as e:
            raise BadRequestError(f"User {e}") from e
