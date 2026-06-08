# kronicle/db/migration/persistence/state_writer.py
# pyright: reportUnusedImport=false
# Migration status tables (IMPORTANT: force model registration)
from kronicle.db.migration.persistence.schema_migration_history import (  # noqa
    CoreSchemaMigrationHistory,
    RbacSchemaMigrationHistory,
)

from kronicle.db.migration.persistence.schema_migration_state import (  # noqa
    CoreSchemaMigrationState,
    RbacSchemaMigrationState,
)
