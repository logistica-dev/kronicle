# kronicle/db/registry.py

# Schemas
from kronicle.db.core.models.core_entity import CoreEntity  # noqa
from kronicle.db.rbac.models.rbac_entity import RbacEntity  # noqa

# Migration status tables (IMPORTANT: force model registration)
from kronicle.db.migration.persistence.schema_migration_history import (  # noqa
    CoreSchemaMigrationHistory,
    RbacSchemaMigrationHistory,
)

from kronicle.db.migration.persistence.schema_migration_state import (  # noqa
    CoreSchemaMigrationState,
    RbacSchemaMigrationState,
)

# Core layer
import kronicle.db.core.models  # noqa
import kronicle.db.core.links  # noqa

# RBAC layer
import kronicle.db.rbac.models  # noqa
import kronicle.db.rbac.links  # noqa


def get_migration_schemas() -> set[str]:
    return {
        CoreEntity.namespace(),
        RbacEntity.namespace(),
    }
