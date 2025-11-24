# Kronicle API

Kronicle is a FastAPI-based time-series storage service with strict separation of admin, writer, and reader permissions.
The API is organized into three main route groups: **Setup/Admin**, **Writer/Data**, and **Reader/API**.

Base URL prefixes include the API version:

- `/setup/v1` → Admin/setup routes
- `/data/v1` → Writer/data routes
- `/api/v1` → Reader/read-only routes

Here is the full updated [API (as an OpenAPI JSON)](docs/openapi.json)

If the server is launched, you can get an interactive Swagger at http://localhost:8000/docs

---

## Setup / Admin Routes (`/setup/{api_version}`)

These routes are intended for administrators or users with full access to manage channels.

| Method | Path                          | Description                                                                   |
| ------ | ----------------------------- | ----------------------------------------------------------------------------- |
| POST   | `/channels`                   | Create a new channel with metadata and schema. Fails if the channel exists.   |
| PUT    | `/channels/{sensor_id}`       | Upsert a channel: updates metadata if exists, otherwise creates it.           |
| PATCH  | `/channels/{sensor_id}`       | Partially update a channel’s metadata, tags, or schema.                       |
| DELETE | `/channels/{sensor_id}`       | Delete a channel and its metadata. All associated data rows are also removed. |
| GET    | `/channels`                   | List all channels with metadata and row counts (admin view).                  |
| DELETE | `/channels/{sensor_id}/rows`  | Delete all data rows for a channel, keeping metadata intact.                  |
| POST   | `/channels/{sensor_id}/clone` | Clone a channel’s schema and optionally metadata. Does not copy data rows.    |
| GET    | `/channels/columns/types`     | List the types available to describe the columns.                             |

---

## Writer / Data Routes (`/data/{api_version}`)

These routes are primarily for appending sensor data and managing metadata safely. Writers have read-only access for exploration.

### Append-only Endpoints

| Method | Path                         | Description                                                       |
| ------ | ---------------------------- | ----------------------------------------------------------------- |
| POST   | `/channels`                  | Upsert metadata and insert rows. Auto-creates channel if missing. |
| POST   | `/channels/{sensor_id}/rows` | Insert new rows for an existing sensor. Metadata is not modified. |

### Read-only Endpoints (accessible to writers)

| Method | Path                            | Description                                           |
| ------ | ------------------------------- | ----------------------------------------------------- |
| GET    | `/channels`                     | Fetch metadata for all sensors.                       |
| GET    | `/channels/{sensor_id}`         | Fetch metadata for a specific sensor.                 |
| GET    | `/channels/{sensor_id}/rows`    | Fetch all rows and metadata for a specific sensor.    |
| GET    | `/channels/{sensor_id}/columns` | Fetch all columns and metadata for a specific sensor. |

---

## Reader / API Routes (`/api/{api_version}`)

These routes are read-only and safe for public or restricted clients.

| Method | Path                            | Description                                                  |
| ------ | ------------------------------- | ------------------------------------------------------------ |
| GET    | `/channels`                     | List all available sensor channels with metadata (no rows).  |
| GET    | `/channels/{sensor_id}`         | Fetch metadata for a specific sensor (no rows).              |
| GET    | `/channels/{sensor_id}/rows`    | Fetch stored rows along with metadata for a specific sensor. |
| GET    | `/channels/{sensor_id}/columns` | Fetch stored rows along with metadata for a specific sensor. |

---

## Notes

- **Immutable Data:** Sensor data rows are append-only once inserted.
- **Metadata:** Includes schema, tags, and additional sensor metadata.
- **Permissions:**
  - Setup routes require admin access.
  - Writer routes allow appending data plus safe reads.
  - Reader routes are read-only.

---

## Prerequisites

### Install Postgresql@17 and TimescaleDB (MacOS version here)

```sh
brew install postgresql@17 timescaledb

# Start Postgres
brew services start postgresql@17

# You can check if it is running correctly with
pg_ctl -D /opt/homebrew/var/postgresql@17 status
# or
brew services list

# Add this is your ~/.zshrc
export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"
export PGDATA="/opt/homebrew/var/postgresql@17"

# Then
source ~/.zshrc


# By default, postgres role is created. You can create your own user and DB
createuser --interactive            # prompts for username & superuser? yes/no
createdb mydb              # creates a database owned by your new user
psql mydb                  # now you can connect to it in psql
```

Inside psql

```sql
-- inside psql
\du         -- list roles/users
\l          -- list databases
```

See additional psql/timescaleDB commands in `./docs`

### Enable TimescaleDB

```sql
-- inside psql:
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
```

If you want it enabled by default for all databases, you can edit postgresql.conf:

`shared_preload_libraries = 'timescaledb'`

Then restart PostgreSQL:

```sh
brew services restart postgresql@17
```

Verify TimescaleDB

````sql
-- inside psql
\dx
# Launch

## FastAPI server

```sh
cd src
# nodemon-like reload-on-code-change server launch
# instead of `fastapi run kronicle/main.py`
uvicorn kronicle.main:app --reload --host 0.0.0.0 --port 8000
````

You can then test the API with Swagger:
http://localhost:8000/docs
