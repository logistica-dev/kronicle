# Centralized list of all RBAC tables
from kronicle.db.core.models.channel import Channel
from kronicle.db.core.models.core_entity import CoreEntity
from kronicle.db.core.models.core_resource import CoreResource
from kronicle.db.core.models.zone import Zone

CORE_NAMESPACE = CoreEntity.namespace()

# Centralized list of all RBAC tables
ALL_CORE_TABLES = [
    Channel,
    # Zone must be created after Channel
    Zone,
    # The view for both of these:
    CoreResource,
]
