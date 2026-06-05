# kronicle/db/rbac/models/rbac_user.py
from __future__ import annotations

from typing import Any

from sqlalchemy import Boolean, Index, String, text
from sqlalchemy.orm import Mapped, mapped_column

from kronicle.db.rbac.models.rbac_entity import RbacEntity


class RbacUser(RbacEntity):
    __tablename__ = "users"

    __table_args__ = (
        Index("ix_users_email", "email"),
        {"schema": RbacEntity.namespace(), "extend_existing": True},
    )

    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)  # Mandatory!
    password_hash: Mapped[str | None] = mapped_column(String(255), nullable=True)

    full_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    external_id: Mapped[str | None] = mapped_column(String(255), unique=True, nullable=True)

    # default -> Python-side default | server_default -> PostgreSQL DB-side default
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default=text("true"), nullable=False)
    is_superuser: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)

    @property
    def snapshot(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "id": str(self.id),
            "email": self.email,
            "name": self.name,
        }
        if self.full_name is not None:
            result["full_name"] = self.full_name
        if not self.is_active:
            result["is_active"] = False
        if self.is_superuser:
            result["is_superuser"] = True
        return result

    def __repr__(self) -> str:
        return f"<User {self.email}>"
