# kronicle/schemas/rbac/safe_user_schemas.py
from __future__ import annotations

from json import dumps
from typing import Any
from uuid import UUID

from pydantic import BaseModel, EmailStr, PrivateAttr, field_validator

from kronicle.auth.pwd.pwd_manager import PasswordManager
from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.errors.error_types import BadRequestError
from kronicle.schemas.rbac.input_user_schemas import (
    _USERNAME_EXTRA_CHARS,
    _USERNAME_MAX_LENGTH,
    _USERNAME_MIN_LENGTH,
    InputUser,
)
from kronicle.utils.str_utils import uuid_to_str, validate_name_syntax

mod = "outusr"


class ProcessedUser(BaseModel):
    """
    Represents validated and processed user data ready for database insertion.
    Contains derived fields (e.g., hashed passwords) and metadata.
    """

    email: EmailStr
    password_hash: str | None = None  # Hashed password (never store raw passwords!)
    name: str | None = None
    external_id: str | None = None
    full_name: str | None = None
    details: dict[str, Any] | None = {"auth_method": "local"}  # Default metadata

    @field_validator("name")
    def validate_user_name_syntax(cls, v: str | None) -> str | None:
        if not v:
            return None
        try:
            return validate_name_syntax(
                v, extra_chars=_USERNAME_EXTRA_CHARS, min_length=_USERNAME_MIN_LENGTH, max_length=_USERNAME_MAX_LENGTH
            )
        except ValueError as e:
            raise BadRequestError(f"User {e}") from e

    @classmethod
    def from_input(cls, data: InputUser):
        hashed = PasswordManager().hash_password(data.password) if data.password else None
        return ProcessedUser(
            email=data.email,
            password_hash=hashed,
            name=data.name,
            full_name=data.full_name,
            external_id=data.orcid,
            details=data.details,  # explicitly derived
        )

    def to_db_user(self) -> RbacUser:
        """Convert this processed user data into a RbacUser for persistence."""
        return RbacUser(
            email=self.email,
            name=self.name,
            password_hash=self.password_hash,
            external_id=self.external_id,
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
    details: dict[str, Any] | None = None
    is_active: bool | None = None

    # Internal attribute, not part of .dict()/JSON by default
    _is_su: bool = PrivateAttr(False)

    @property
    def is_su(self):
        return self._is_su

    def _set_su(self):
        self._is_su = True

    @classmethod
    def from_db(cls, db_user: RbacUser) -> OutputUser:
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
            is_active=db_user.is_active,
        )
        if db_user.is_superuser:
            usr._set_su()
        # log_d(here, "usr.is_superuser", usr.is_su)
        return usr

    # Include is_su in dict/json output
    def model_dump(self, *args, **kwargs) -> dict:
        d = super().model_dump(
            *args,
            exclude_none=True,
            exclude_unset=True,
            **kwargs,
        )
        d["id"] = uuid_to_str(self.id)
        # if not self.details:
        #     d.pop("details", None)
        if self.is_active:
            d.pop("is_active", None)
        if self._is_su:
            d["is_su"] = True
        else:
            d.pop("is_su", None)
        return d

    # Include is_su in JSON output
    def model_dump_json(self, *args, **kwargs):
        return dumps(self.model_dump())

    def __str__(self) -> str:
        return f"OutUser {self.model_dump_json()}"
