#!/bin/bash

TIMESCALEDB_IMG="timescale/timescaledb:latest-pg17"

CONTAINER_NAME=kronicle
PORT=5432

DB_USR=kronicle_usr
DB_PWD=kronicle_pwd
DB_NAME=kronicle_db
DB_DIR="$HOME/kronicle/psql"

# Launch the Timescale DB
podman run -d                       \
    --name $CONTAINER_NAME          \
    -p $PORT:$PORT                  \
    -e POSTGRES_USER=$DB_USR        \
    -e POSTGRES_PASSWORD=$DB_PWD    \
    -e POSTGRES_DB=$DB_NAME         \
    -v timescale_data:"$DB_DIR"     \
    $TIMESCALEDB_IMG

podman exec -it $CONTAINER_NAME psql -U $DB_USR -d $DB_NAME

# Stop the Timescale DB
docker stop $CONTAINER_NAME && docker rm $CONTAINER_NAME

# Send request to the Timescale DB
psql -h localhost -p $PORT -U "$DB_USR" -d "$DB_NAME"
