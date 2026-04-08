# kronicle/db/core/models/__init__.py

# Centralized list of all RBAC tables
from kronicle.db.core.models.core_channel import Channel
from kronicle.db.core.models.core_entity import CoreEntity
from kronicle.db.core.models.core_resource import CoreResource
from kronicle.db.core.models.core_row import Row
from kronicle.db.core.models.core_zone import Zone

CORE_NAMESPACE = CoreEntity.namespace()

# Centralized list of all RBAC tables
ALL_CORE_TABLES = [
    Row,
    # Channel must be created after Row
    Channel,
    # Zone must be created after Channel
    Zone,
    # The view for both of these:
    CoreResource,
]
