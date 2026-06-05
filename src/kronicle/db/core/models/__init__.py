# kronicle/db/core/models/__init__.py

from kronicle.db.core.links.zone_hierarchy import ZoneHierarchy
from kronicle.db.core.models.core_channel import Channel
from kronicle.db.core.models.core_entity import CoreEntity
from kronicle.db.core.models.core_resource import CoreResource
from kronicle.db.core.models.core_row import Row
from kronicle.db.core.models.core_zone import Zone

CORE_NAMESPACE = CoreEntity.namespace()

# Centralized list of all CORE tables
ALL_CORE_TABLES = [
    Zone,
    # Zone must be created before Channel
    Channel,
    # Channel must be created before Row
    Row,
    # The view for both of these:
    CoreResource,
    ZoneHierarchy,
]
