# scripts/migrations/env.py
import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import create_async_engine

from kronicle.db.base.kronicle_base import KronicleBase
from kronicle.db.migration.bootstrap_check import run_bootstrap_checks
from kronicle.db.registry import *  # noqa
from kronicle.deps.settings import KronicleSettings

# This is the Alembic Config object, which provides access to .ini values
config = context.config

# Setup logging from .ini
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Read RBAC DB URL from your conf
conf = KronicleSettings()
db_url = conf.db.rbac_connection_url

# Set the SQLAlchemy URL dynamically
config.set_main_option("sqlalchemy.url", db_url)

# Metadata to target
target_metadata = KronicleBase.metadata


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
    connectable = create_async_engine(db_url, poolclass=pool.NullPool)

    async with connectable.connect() as connection:

        # Bootstrap safety layer
        await connection.run_sync(run_bootstrap_checks)

        # ONLY if validation passes
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
