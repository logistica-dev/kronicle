# kronicle/repo/core/zone_hierarchy_repo.py
from sqlalchemy.orm import Session

from kronicle.db.core.links.zone_hierarchy import ZoneHierarchy
from kronicle.db.core.models.core_zone import Zone
from kronicle.repo.kronicle_link_repo import KronicleLinkRepository


class ZoneHierarchyRepository(KronicleLinkRepository[ZoneHierarchy]):
    model = ZoneHierarchy

    # --- Minimal persistence API ---
    def add_parent_zone(self, db: Session, parent: Zone, child: Zone):
        self.ensure_link(db, {self.model.PARENT_ID: parent.id, self.model.CHILD_ID: child.id})

    def remove_parent_zone(self, db: Session, parent: Zone, child: Zone):
        self.remove_link(db, {self.model.PARENT_ID: parent.id, self.model.CHILD_ID: child.id})
