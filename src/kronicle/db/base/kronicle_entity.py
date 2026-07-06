# kronicle/db/base/kronicle_entity.py
from json import dumps
from typing import Any

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from kronicle.db.base.kronicle_base import KronicleBase
from kronicle.utils.str_utils import uuid_to_str


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

    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)

    @property
    def row_snapshot(self) -> dict[str, Any]:
        """
        Return a minimal, JSON-serializable representation of this **row** for audit/logging purposes.
        This does **not** represent the full database or table state.
        """
        return {
            "id": uuid_to_str(self.id),
            "name": self.name if self.name else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "details": self.details if self.details else None,
        }

    def __str__(self) -> str:
        return f"{type(self).__name__} {{'id': {self.id}, 'name': {self.name}, 'details': {dumps(self.details)}}}"
        # return super().__str__()
