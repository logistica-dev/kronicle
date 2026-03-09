# migrations/env.py
import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from kronicle.db.rbac.models.rbac_entity import RbacEntity  # your declarative base

# Import your app config
from kronicle.deps.settings import Settings

# This is the Alembic Config object, which provides access to .ini values
config = context.config

# Setup logging from .ini
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Read RBAC DB URL from your conf
conf = Settings()
db_url = conf.rbac.connection_url

# Set the SQLAlchemy URL dynamically
config.set_main_option("sqlalchemy.url", db_url)

# Metadata to target
target_metadata = RbacEntity.meta


# -------------------------------------------------------
# Migration functions
# -------------------------------------------------------


def run_migrations_offline():
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online():
    """
    Run migrations in 'online' (async) mode.

    [Note] Offline mode: Alembic writes SQL to stdout or a file; you don’t need a live connection.
           The app can be running or not, it doesn’t matter for generating the script.
    """
    connectable: AsyncEngine = create_async_engine(db_url, poolclass=pool.NullPool)

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def do_run_migrations(connection):
    """Sync wrapper called by async engine."""
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


# -------------------------------------------------------
# Entry point
# -------------------------------------------------------

if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
