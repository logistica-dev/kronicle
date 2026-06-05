#!/bin/bash
set -euo pipefail
echo -- D Launched from $(pwd)

# Charge les secrets et exporte PGPASSWORD pour les commandes pg_*
source .conf/.secrets
export PGPASSWORD="${POSTGRES_PASSWORD}"
export PYTHONPATH="${PYTHONPATH:-}${PYTHONPATH:+:}$(pwd)"

# 1. Supprime et recree la base
dropdb -U postgres -h localhost kronicle_db_nu
createdb -U postgres -h localhost kronicle_db_nu

# 2. Bootstrap: users + schemas (avec owners + droits) + TimescaleDB
.venv/bin/python scripts/init/01_bootstrap_db.py

# 3. Restaure tables + indexes + data (ownership re-appliquee depuis le dump)
pg_restore -U postgres -h localhost -d kronicle_db_nu \
  "$(pwd)/$1"

# 4. Backup URL pour que le backup de pre-migration ait tous les droits
export KRONICLE_BACKUP_URL="postgresql://postgres:${POSTGRES_PASSWORD}@localhost:5432/kronicle_db_nu"

# 5. Lance la migration
.venv/bin/python -m kronicle.db.migration.migration_manager
