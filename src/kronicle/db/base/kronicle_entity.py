# kronicle/db/base/kronicle_entity.py
from typing import Any

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from kronicle.db.base.kronicle_base import KronicleBase


class KronicleEntity(KronicleBase):
    """
    Base class for all Core and RBAC tables.
    Provides:
      - Primary UUID key
      - Created/Updated timestamps
      - Flexible JSONB details
      - Table structure validation at startup
    """

    __abstract__ = True  # Do not create a table for this class itself

    # Optional name
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)

    @property
    def row_snapshot(self) -> dict[str, Any]:
        """
        Return a minimal, JSON-serializable representation of this **row** for audit/logging purposes.
        This does **not** represent the full database or table state.
        """
        return {
            "id": str(self.id),
            "name": self.name,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "details": self.details,
        }
