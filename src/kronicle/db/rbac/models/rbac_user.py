# kronicle/db/rbac/models/rbac_user.py
from __future__ import annotations

from typing import Any

from pydantic import EmailStr
from sqlalchemy import Boolean, Index, String, text
from sqlalchemy.orm import Mapped, Session, mapped_column

from kronicle.db.rbac.models.rbac_entity import RbacEntity


class RbacUser(RbacEntity):
    __tablename__ = "users"

    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)  # Mandatory!
    password_hash: Mapped[str | None] = mapped_column(String(255), nullable=True)

    full_name: Mapped[str | None] = mapped_column(String(255), unique=True, nullable=True)
    external_id: Mapped[str | None] = mapped_column(String(255), unique=True, nullable=True)

    # default -> Python-side default | server_default -> PostgreSQL DB-side default
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("true"))
    is_superuser: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")

    __table_args__ = (
        Index("ix_users_email", "email"),
        {"schema": RbacEntity.namespace(), "extend_existing": True},
    )

    @property
    def snapshot(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "email": self.email,
            "name": self.name,
            "full_name": self.full_name,
            "is_active": self.is_active,
            "is_superuser": self.is_superuser,
        }

    def __repr__(self) -> str:
        return f"<User {self.email}>"

    # ----------------------------------------------------------------------------------------------
    # Read table
    # ----------------------------------------------------------------------------------------------
    @classmethod
    def fetch_by_email(cls, db: Session, email: EmailStr) -> RbacUser | None:
        return db.query(cls).filter(cls.email == email).first()

    @classmethod
    def fetch_by_name(cls, db: Session, name: str) -> RbacUser | None:
        return db.query(cls).filter(cls.name == name).first()

    @classmethod
    def fetch_all(cls, db: Session) -> list[RbacUser]:
        return db.query(RbacUser).filter(RbacUser.is_superuser.is_(False)).all()

    @classmethod
    def get_by_login(cls, db: Session, login: str, by_email: bool = True) -> "RbacUser | None":
        """
        Fetch a user by email or username.

        Args:
            db: SQLAlchemy session
            login: email or username string
            by_email: if True, filter by email; else filter by username

        Returns:
            RbacUser instance if found, else None
        """
        field = cls.email if by_email else cls.name
        return db.query(cls).filter(field == login).first()
