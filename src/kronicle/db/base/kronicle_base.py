# kronicle/db/base/kronicle_base.py
from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from kronicle.db.base.kronicle_table import KronicleTable

mod = "kron_base"


class KronicleBase(KronicleTable):
    """
    Base class for all Core and RBAC tables.
    Provides:
      - Primary UUID key
      - Created/Updated timestamps
      - Flexible JSONB details
      - Table structure validation at startup
    """

    __abstract__ = True  # Do not create a table for this class itself

    # Primary key UUID
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        server_default=text("gen_random_uuid()"),
    )
