# kronicle/repo/hierarchy/zone_hierarchy_repo.py

from kronicle.db.core.links.zone_hierarchy import ZoneHierarchy
from kronicle.db.core.models.core_zone import CoreZone
from kronicle.repo.hierarchy.hierarchy_repo import KronicleHierarchyRepo


class ZoneHierarchyRepository(KronicleHierarchyRepo[ZoneHierarchy, CoreZone]):
    model = ZoneHierarchy
    node_model = CoreZone
