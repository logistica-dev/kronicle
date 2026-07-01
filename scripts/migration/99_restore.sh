#!/bin/bash
set -euo pipefail
echo -- D Launched from $(pwd)

# Charge les secrets et exporte PGPASSWORD pour les commandes pg_*
source .conf/.secrets
export PGPASSWORD="${POSTGRES_PASSWORD}"
export PYTHONPATH="${PYTHONPATH:-}${PYTHONPATH:+:}$(pwd)"

# 1. Supprime et recree la base
dropdb -U $POSTGRES_USER -h "$KRONICLE_DB_HOST" "$KRONICLE_DB_NAME"
createdb -U $POSTGRES_USER -h "$KRONICLE_DB_HOST" "$KRONICLE_DB_NAME"

# 2. Bootstrap: users + schemas (avec owners + droits) + TimescaleDB
.venv/bin/python scripts/init/01_bootstrap_db.py

# 3. Restaure tables + indexes + data (ownership re-appliquee depuis le dump)
pg_restore -U $POSTGRES_USER -h "$KRONICLE_DB_HOST" -d "$KRONICLE_DB_NAME" \
  "$(pwd)/$1"

# 4. Backup URL pour que le backup de pre-migration ait tous les droits
export KRONICLE_BACKUP_URL="postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$KRONICLE_DB_HOST:$KRONICLE_DB_PORT/$KRONICLE_DB_NAME"

# 5. Lance la migration
.venv/bin/python -m kronicle.db.migration.migration_manager


# --------------------------------------------------------------------------------------------------
# This are commands when the migration manager failed to restore the DB:
psql -d "$KRONICLE_BACKUP_URL" -c "
DO \$\$
DECLARE
  r RECORD;
BEGIN
  FOR r IN SELECT conname, conrelid::regclass AS tbl
           FROM pg_constraint
           WHERE contype = 'f'
             AND connamespace IN ('rbac'::regnamespace, 'core'::regnamespace)
  LOOP
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', r.tbl, r.conname);
  END LOOP;
END \$\$;
"

pg_restore --clean --if-exists --no-owner \
  -d "$KRONICLE_BACKUP_URL" \
  backup/kronicle_20260701_075949.dump

psql -d "$KRONICLE_BACKUP_URL" -c "GRANT USAGE ON SCHEMA core TO kronicle_rbac_usr;"
psql -d "$KRONICLE_BACKUP_URL" -c "GRANT USAGE ON SCHEMA rbac TO kronicle_rbac_usr;"

psql -d "$KRONICLE_BACKUP_URL" -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA core TO kronicle_rbac_usr;"
psql -d "$KRONICLE_BACKUP_URL" -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA rbac TO kronicle_rbac_usr;"

psql -d "$KRONICLE_BACKUP_URL" -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA core TO kronicle_rbac_usr;"
psql -d "$KRONICLE_BACKUP_URL" -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA rbac TO kronicle_rbac_usr;"

# --------------------------------------------------------------------------------------------------
# Identify possible orphans

psql -d "$KRONICLE_BACKUP_URL" -c "
-- core schema
SELECT 'core.channels' AS table_name, count(*) AS orphan_rows
FROM core.channels c LEFT JOIN core.zones z ON c.zone_id = z.id WHERE c.zone_id IS NOT NULL AND z.id IS NULL
UNION ALL
SELECT 'core.rows', count(*)
FROM core.rows r LEFT JOIN core.channels c ON r.channel_id = c.id WHERE c.id IS NULL
UNION ALL
SELECT 'core.zone_hierarchy.child_id', count(*)
FROM core.zone_hierarchy zh LEFT JOIN core.zones z ON zh.child_id = z.id WHERE z.id IS NULL
UNION ALL
SELECT 'core.zone_hierarchy.parent_id', count(*)
FROM core.zone_hierarchy zh LEFT JOIN core.zones z ON zh.parent_id = z.id WHERE z.id IS NULL

UNION ALL

-- rbac schema
SELECT 'rbac.channel_access_profiles.channel_id', count(*)
FROM rbac.channel_access_profiles cap LEFT JOIN core.channels c ON cap.channel_id = c.id WHERE c.id IS NULL
UNION ALL
SELECT 'rbac.channel_access_profiles.role_id', count(*)
FROM rbac.channel_access_profiles cap LEFT JOIN rbac.roles r ON cap.role_id = r.id WHERE r.id IS NULL
UNION ALL
SELECT 'rbac.channel_policies.access_profile_id', count(*)
FROM rbac.channel_policies cp LEFT JOIN rbac.channel_access_profiles cap ON cp.access_profile_id = cap.id WHERE cap.id IS NULL
UNION ALL
SELECT 'rbac.channel_policies.subject_id', count(*)
FROM rbac.channel_policies cp LEFT JOIN rbac.subjects s ON cp.subject_id = s.id WHERE s.id IS NULL
UNION ALL
SELECT 'rbac.group_hierarchy.child_id', count(*)
FROM rbac.group_hierarchy gh LEFT JOIN rbac.groups g ON gh.child_id = g.id WHERE g.id IS NULL
UNION ALL
SELECT 'rbac.group_hierarchy.parent_id', count(*)
FROM rbac.group_hierarchy gh LEFT JOIN rbac.groups g ON gh.parent_id = g.id WHERE g.id IS NULL
UNION ALL
SELECT 'rbac.group_roles.group_id', count(*)
FROM rbac.group_roles gr LEFT JOIN rbac.groups g ON gr.group_id = g.id WHERE g.id IS NULL
UNION ALL
SELECT 'rbac.group_roles.role_id', count(*)
FROM rbac.group_roles gr LEFT JOIN rbac.roles r ON gr.role_id = r.id WHERE r.id IS NULL
UNION ALL
SELECT 'rbac.row_access_profiles.role_id', count(*)
FROM rbac.row_access_profiles rap LEFT JOIN rbac.roles r ON rap.role_id = r.id WHERE r.id IS NULL
UNION ALL
SELECT 'rbac.row_access_profiles.row_id', count(*)
FROM rbac.row_access_profiles rap LEFT JOIN core.rows r ON rap.row_id = r.id WHERE r.id IS NULL
UNION ALL
SELECT 'rbac.row_policies.access_profile_id', count(*)
FROM rbac.row_policies rp LEFT JOIN rbac.row_access_profiles rap ON rp.access_profile_id = rap.id WHERE rap.id IS NULL
UNION ALL
SELECT 'rbac.row_policies.subject_id', count(*)
FROM rbac.row_policies rp LEFT JOIN rbac.subjects s ON rp.subject_id = s.id WHERE s.id IS NULL
UNION ALL
SELECT 'rbac.subjects.group_id', count(*)
FROM rbac.subjects s LEFT JOIN rbac.groups g ON s.group_id = g.id WHERE s.group_id IS NOT NULL AND g.id IS NULL
UNION ALL
SELECT 'rbac.subjects.user_id', count(*)
FROM rbac.subjects s LEFT JOIN rbac.users u ON s.user_id = u.id WHERE s.user_id IS NOT NULL AND u.id IS NULL
UNION ALL
SELECT 'rbac.user_groups.group_id', count(*)
FROM rbac.user_groups ug LEFT JOIN rbac.groups g ON ug.group_id = g.id WHERE g.id IS NULL
UNION ALL
SELECT 'rbac.user_groups.user_id', count(*)
FROM rbac.user_groups ug LEFT JOIN rbac.users u ON ug.user_id = u.id WHERE u.id IS NULL
UNION ALL
SELECT 'rbac.user_roles.role_id', count(*)
FROM rbac.user_roles ur LEFT JOIN rbac.roles r ON ur.role_id = r.id WHERE r.id IS NULL
UNION ALL
SELECT 'rbac.user_roles.user_id', count(*)
FROM rbac.user_roles ur LEFT JOIN rbac.users u ON ur.user_id = u.id WHERE u.id IS NULL
UNION ALL
SELECT 'rbac.zone_access_profiles.role_id', count(*)
FROM rbac.zone_access_profiles zap LEFT JOIN rbac.roles r ON zap.role_id = r.id WHERE r.id IS NULL
UNION ALL
SELECT 'rbac.zone_access_profiles.zone_id', count(*)
FROM rbac.zone_access_profiles zap LEFT JOIN core.zones z ON zap.zone_id = z.id WHERE z.id IS NULL
UNION ALL
SELECT 'rbac.zone_policies.access_profile_id', count(*)
FROM rbac.zone_policies zp LEFT JOIN rbac.zone_access_profiles zap ON zp.access_profile_id = zap.id WHERE zap.id IS NULL
UNION ALL
SELECT 'rbac.zone_policies.subject_id', count(*)
FROM rbac.zone_policies zp LEFT JOIN rbac.subjects s ON zp.subject_id = s.id WHERE s.id IS NULL
ORDER BY 1;
" 2>&1

# Further identify the culprits
psql -d "$KRONICLE_BACKUP_URL" -c "
SELECT gr.id, gr.group_id, gr.role_id, gr.created_at, gr.details
FROM rbac.group_roles gr
LEFT JOIN rbac.groups g ON gr.group_id = g.id
WHERE g.id IS NULL;
" 2>&1

# Delete orphans
psql -d "$KRONICLE_BACKUP_URL" -c "
DELETE FROM rbac.group_roles WHERE group_id NOT IN (SELECT id FROM rbac.groups);
SELECT count(*) AS orphan_count FROM rbac.group_roles WHERE group_id NOT IN (SELECT id FROM rbac.groups);
" 2>&1
