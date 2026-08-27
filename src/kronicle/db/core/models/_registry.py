# kronicle/db/core/models/_registry.py

from kronicle.db.core.links.zone_hierarchy import ZoneHierarchy
from kronicle.db.core.models.core_channel import CoreChannel
from kronicle.db.core.models.core_entity import CoreEntity
from kronicle.db.core.models.core_row import CoreRow
from kronicle.db.core.models.core_zone import CoreZone

CORE_NAMESPACE = CoreEntity.namespace()

# Centralized list of all CORE tables
ALL_CORE_TABLES = [
    CoreZone,
    # Zone must be created before Channel
    CoreChannel,
    # Channel must be created before Row
    CoreRow,
    # The view for both of these:
    ZoneHierarchy,
]
