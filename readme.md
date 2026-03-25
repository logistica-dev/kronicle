# Kronicle

Kronicle is a FastAPI-based time-series measurements storage service with strict separation of admin, writer, and reader permissions.

## Launching the app

The app can run itself in init mode if it detects that no base is present in the database.
In such condition, here are the resquested information:

- `POSTGRES_USER`: name of the DB superuser
- `POSTGRES_PASSWORD`: password of the DB superuser (in clear)
- `POSTGRES_DB`: name of the DB to create

- `KRONICLE_SU_INFO`: credentials for the Kronicle app superuser that will be needed to interact with the API
  This is a base64url-encoded triplet of <su_name>:<su_email>:<argon2_hashed_pwd>
  A script is provided to help with this this easier:

```sh
python3 ./scripts/utils/hash_creds.py su_name su_email "SU_passw0rd"
```

These informations are also required by the init phase. They will need to be provided in a production run as well:

- `KRONICLE_CHAN_CREDS`: base64url-encoded <usr>:<pwd> credentials of the user that will manage the data (metadata, timeseries) in the DB
- `KRONICLE_RBAC_CREDS`: base64url-encoded <usr>:<pwd> credentials of the user that will manage the RBAC in the DB ("role-base access control" = authorization in this app)

- `KRONICLE_HOST` (defaulted to 0.0.0.0): host for the app server
- `KRONICLE_PORT` (defaulted to 8000): listening port for the app server
- `KRONICLE_ENV`: set to test for the docs to be

You can for instance set all these variables in a `.env` file

```sh
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=kronicle_db

KRONICLE_SU_INFO=S3JvbmljbGVBZG1pbjphZG1pbkBrcm9uaWNsZS5hcHA6JGFyZ29uMmlkJHY9MTkkbT02NTUzNix0PTMscD00JGVkSE56VDc2YjRXUitiMEowTDRObFEkbkZRNVJqclkreG56QkMzbnZma2Z2OEI0N1YvY2lwbGF5Y1VGUUtzS2pZaw

KRONICLE_CHAN_CREDS=Y2hhbl91c3JfbmFtZTpjaGFuX3Vzcl9wYXNz
KRONICLE_RBAC_CREDS=cmJhY191c3JfbmFtZTpyYmFjX3Vzcl9wYXNz

KRONICLE_PORT=8765
KRONICLE_HOST=localhost
KRONICLE_ENV=test
KRONICLE_LOG_LEVEL=3 # 3=debug, 2=info, 1=warn, 0=error
```

Then launch docker-compose (or alternatively podman-compose, whichever you have installed already)

```sh
podman-compose --env-file .env up # -d
```

## OpenAPI/Swagger doc

Launching the app with the environment variable above will display some logs.
You should see a phase of table validation and in the end:

```
D [app.launch] Swagger docs available at: http://localhost:8765/docs
D [app.launch] Kronicle server ready
```

You can go to the URL above to access the OpenAPI documentation: it is powered with the Swagger UI and let you interact with the Kronicle API.
You will need to log as a user: if the app was initialized with the credentials for the Kronicle app superuser given in the example above, try login with KronicleAdmin / KronicleAdmin_Passw0rd
Once logged in, you can copy the access token string, click on one of the locks of the requests bellow, and enter the token.

## API

The API is organized into three main route groups: **Reader/API**, **Writer/Data** and **Setup/Admin**.

Base URL prefixes include the API version:

- `/api/v1` → Reader/read-only routes
- `/data/v1` → Writer/data routes
- `/setup/v1` → Admin/setup routes

Here is the full updated [API (as an OpenAPI JSON)](docs/openapi.json)

If the server is launched, you can get an interactive Swagger at http://localhost:8000/docs

### Query filters

| Query | Description                                                                | Allowed Column Types                                                                | SQL Operator        | DB value is cast            |
| ----- | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ------------------- | --------------------------- |
| `col` | Exact match. For dict subkeys, value is treated as text.                   | Any scalar type (`int`, `float`, `str`, `datetime`, etc.), dict subkeys (text only) | `=`                 | No                          |
| `min` | Minimum value comparison. **Not allowed on dict subkeys or lists**.        | Numeric (`int`, `float`) or datetime (`datetime`, `timestamptz`, `timetz`)          | `>=`                | `::numeric`/`::timestamptz` |
| `max` | Maximum value comparison. **Not allowed on dict subkeys or lists**.        | Numeric (`int`, `float`) or datetime (`datetime`, `timestamptz`, `timetz`)          | `<=`                | `::numeric`/`::timestamptz` |
| `any` | Checks if value is in a list of values. For dict subkeys, treated as text. | Any scalar type, dict subkeys (text only)                                           | `IN`                | No                          |
| `has` | Checks if the list column contains the specified value(s).                 | List columns only                                                                   | `@>` / `= ANY(...)` | No                          |

---

### Setup / Admin Routes (`/setup/{api_version}`)

These routes are intended for administrators or users with full access to manage channels.

| Method | Path                           | Description                                                                   |
| ------ | ------------------------------ | ----------------------------------------------------------------------------- |
| POST   | `/channels`                    | Create a new channel with metadata and schema. Fails if the channel exists.   |
| PUT    | `/channels/{channel_id}`       | Upsert a channel: updates metadata if exists, otherwise creates it.           |
| PATCH  | `/channels/{channel_id}`       | Partially update a channel’s metadata, tags, or schema.                       |
| DELETE | `/channels/{channel_id}`       | Delete a channel and its metadata. All associated data rows are also removed. |
| GET    | `/channels`                    | List all channels with metadata and row counts (admin view).                  |
| DELETE | `/channels/{channel_id}/rows`  | Delete all data rows for a channel, keeping metadata intact.                  |
| POST   | `/channels/{channel_id}/clone` | Clone a channel’s schema and optionally metadata. Does not copy data rows.    |
| GET    | `/channels/columns/types`      | List the types available to describe the columns.                             |

---

### Writer / Data Routes (`/data/{api_version}`)

These routes are primarily for appending channel data and managing metadata safely. Writers have read-only access for exploration.

#### Append-only Endpoints

| Method | Path                          | Description                                                        |
| ------ | ----------------------------- | ------------------------------------------------------------------ |
| POST   | `/channels`                   | Upsert metadata and insert rows. Auto-creates channel if missing.  |
| POST   | `/channels/{channel_id}/rows` | Insert new rows for an existing channel. Metadata is not modified. |

#### Read-only Endpoints (accessible to writers)

| Method | Path                             | Description                                            |
| ------ | -------------------------------- | ------------------------------------------------------ |
| GET    | `/channels`                      | Fetch metadata for all channels.                       |
| GET    | `/channels/{channel_id}`         | Fetch metadata for a specific channel.                 |
| GET    | `/channels/{channel_id}/rows`    | Fetch all rows and metadata for a specific channel.    |
| GET    | `/channels/{channel_id}/columns` | Fetch all columns and metadata for a specific channel. |

---

## Reader / API Routes (`/api/{api_version}`)

These routes are read-only and safe for public or restricted clients.

| Method | Path                             | Description                                                   |
| ------ | -------------------------------- | ------------------------------------------------------------- |
| GET    | `/channels`                      | List all available channel channels with metadata (no rows).  |
| GET    | `/channels/{channel_id}`         | Fetch metadata for a specific channel (no rows).              |
| GET    | `/channels/{channel_id}/rows`    | Fetch stored rows along with metadata for a specific channel. |
| GET    | `/channels/{channel_id}/columns` | Fetch stored rows along with metadata for a specific channel. |

---

## Notes

- **Immutable Data:** channel data rows are append-only once inserted.
- **Metadata:** Includes schema, tags, and additional channel metadata.
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
createuser --interactive    # prompts for username & superuser? yes/no
createdb mydb               # creates a database owned by your new user
psql mydb                   # now you can connect to it in psql
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

```sql
-- inside psql
\dx
```

# Launch

## FastAPI server

```sh
cd src
# nodemon-like reload-on-code-change server launch
# instead of `fastapi run kronicle/main.py`
uvicorn kronicle.main:app --reload --host 0.0.0.0 --port 8000
```

You can then test the API with Swagger:
http://localhost:8000/docs
