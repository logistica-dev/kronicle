# kronicle/db/core/models/zone.py
from typing import Any
from uuid import UUID

from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from kronicle.db.base.kronicle_hierarchy import KronicleHierarchyMixin
from kronicle.db.core.models.core_entity import CoreEntity


class Zone(CoreEntity, KronicleHierarchyMixin):
    __tablename__ = "zones"

    # List of channel UUIDs for this zone
    channel_ids: Mapped[list[UUID]] = mapped_column(
        ARRAY(PgUUID(as_uuid=True)),
        nullable=False,
        default=list,
        server_default="{}",
    )

    @property
    def snapshot(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "name": self.name,
            "channel_ids": [str(cid) for cid in getattr(self, "channel_ids", [])],
        }

    # ------------------------
    # Zone-specific helpers
    # ------------------------
    def get_all_channel_ids(self) -> set[UUID]:
        """Recursively collect all channel_ids from this zone and its children."""
        result: set[UUID] = set()
        self._walk_subtree(lambda node: result.update(node.channel_ids))
        return result


# Setup the hierarchy table and children relationship dynamically
# Zone._setup_hierarchy()
