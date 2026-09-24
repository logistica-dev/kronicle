# Kronicle

FastAPI × TimescaleDB microservice for storing time-series measurements.

Kronicle organises data into **channels** (named streams with user-defined schemas), stores rows in append-only TimescaleDB hypertables, and enforces access control through a role-based system with **zones** as isolation boundaries.

## Installation

### Using docker/podman image (recommended)

Copy the `./docker-compose.yml` file provided in a folder

```sh
wk_dir='~/dev/kronicle'
mkdir -p "$wk_dir"
cp ./docker-compose.yml "$wk_dir"
cd "$wk_dir"
```

then launch the container:

```sh
# This will
# - fetch the timescaledb image
# - fetch the latest kronicle image
# - create a `backup` dir
# - create a `kronicle-db-data` dir
# - launch the app
podman-compose up -d
podman logs kronicle-app
```

To stop the container, just do

```sh
podman-compose down
```

### Optional .env file

You may create an `.env` file if you need to set the env variables you'll find in the `docker-compose.yml` as `${VARIABLE:-default_val}`

Place this .env file next to the `./docker-compose.yml` before launching it.

Note: these variables are provided with defaults in the `./docker-compose.yml` file, so it's not absolutely needed for a basic test.

```ini

#--- DB info
# If you need to reinit or do some tests, easiest is rename the DB
POSTGRES_DB="kronicle_db"

#--- App info
# This is the port for incoming requests, outside the container
KRONICLE_HOST_PORT=8888
KRONICLE_BACKUP_HOST_DIR=".backup"

#--- App super user
# These is how you can act on the app!
# You'll need this to hash the app super-user's credentials and email:
#   python3 ./scripts/utils/hash_creds.py kronicle_maintainer kronicle_maintainer@irisa.fr Feb58bc0-a40c-4e6f-a8d2-11d641876886
KRONICLE_SU_INFO="a3JvbmljbGVfbWFpbnRhaW5lcjprcm9uaWNsZV9tYWludGFpbmVyQGlyaXNhLmZyOiRhcmdvbjJpZCR2PTE5JG09NjU1MzYsdD0zLHA9NCRyYkdpRHNxRnYwNVpNSmJBb0VLQTVnJHQwc0ZZV3FXaEwyTjJkWThvazNUNGZpQ3QrNkhVV2tzbW5DZ2pCRVI5dHM"
```

## Key Concepts

| Concept     | What it is                                                                                                                                                           |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Channel** | A named stream of time-series data with a user-defined schema. Each channel maps to its own TimescaleDB hypertable (`channel_{uuid}`).                               |
| **Schema**  | Column definitions as `{name: type}`. Supported types: `str`, `int`, `float`, `bool`, `uuid`, `datetime`, `dict`, `list`. Types can be wrapped with `optional[...]`. |
| **Zone**    | A workspace or project boundary. Zones act as RBAC domains — permissions assigned at the zone level apply to all channels created inside it.                         |
| **Row**     | A single data point in a channel. Rows are append-only once inserted.                                                                                                |

## Quick Start

The fastest way to get started is the interactive notebook: **[README.pynb](README.pynb)**

It walks through pulling the image, running the server, creating channels, writing data, and setting up RBAC — all with executable code cells.

## Installation

### Using docker/podman image (recommended)

(Optional) Create an `.env` file if you need to set the env variables.
Note: these variables are provided with defaults in the `./docker-compose.yml` file, that is sufficient for testing purpose.

```ini

#--- DB info
# If you need to reinit or do some tests, easiest is rename the DB
POSTGRES_DB="kronicle_db"

#--- App info
# This is the port for incoming requests, outside the container
KRONICLE_HOST_PORT=8888
KRONICLE_BACKUP_HOST_DIR=".backup"

#--- App super user
# These is how you can act on the app!
# You'll need this to hash the app super-user's credentials and email:
#   python3 ./scripts/utils/hash_creds.py kronicle_maintainer kronicle_maintainer@irisa.fr Feb58bc0-a40c-4e6f-a8d2-11d641876886
KRONICLE_SU_INFO="a3JvbmljbGVfbWFpbnRhaW5lcjprcm9uaWNsZV9tYWludGFpbmVyQGlyaXNhLmZyOiRhcmdvbjJpZCR2PTE5JG09NjU1MzYsdD0zLHA9NCRyYkdpRHNxRnYwNVpNSmJBb0VLQTVnJHQwc0ZZV3FXaEwyTjJkWThvazNUNGZpQ3QrNkhVV2tzbW5DZ2pCRVI5dHM"
```

Copy your .env file and the `./docker-compose.yml` file provided in a folder then launch the container:

```sh
# This will
# - fetch the timescaledb image
# - fetch the latest kronicle image
# - create a `backup` dir
# - create a `kronicle-db-data` dir
# - launch th eapp
podman-compose up -d
podman logs kronicle-app
```

### Local development

```sh
pip install -e ".[dev]"
cd src
uvicorn kronicle.main:app --reload --host 0.0.0.0 --port 8000
```

Requires a running PostgreSQL instance with TimescaleDB. See [Prerequisites](#prerequisites) below.

## Configuration

Environment variables are the primary configuration method. The essential ones:

| Variable              | Purpose                                                            | Default              |
| --------------------- | ------------------------------------------------------------------ | -------------------- |
| `POSTGRES_USER`       | DB superuser name                                                  | — (required at init) |
| `POSTGRES_PASSWORD`   | DB superuser password                                              | — (required at init) |
| `KRONICLE_DB_NAME`    | Database name                                                      | `kronicle_db`        |
| `KRONICLE_SU_INFO`    | Superuser credentials (base64url-encoded `name:email:argon2_hash`) | — (required at init) |
| `KRONICLE_CHAN_CREDS` | Channel DB user (`base64url user:pass`)                            | — (required)         |
| `KRONICLE_RBAC_CREDS` | RBAC DB user (`base64url user:pass`)                               | — (required)         |
| `KRONICLE_PORT`       | Server port                                                        | `8000`               |
| `KRONICLE_HOST`       | Server bind address                                                | `0.0.0.0`            |
| `KRONICLE_LOG_LEVEL`  | 0=error, 1=warn, 2=info, 3=debug                                   | `2`                  |

Full configuration reference: [`conf/default-conf.ini`](conf/default-conf.ini)

Generate the superuser credentials string:

```sh
python3 ./scripts/utils/hash_creds.py su_name su_email "SU_passw0rd"
```

## API at a Glance

The API is split into four route groups (security lanes):

| Prefix      | Lane           | Purpose                        | Auth         |
| ----------- | -------------- | ------------------------------ | ------------ |
| `/api/v1`   | Consumption    | Read channels, rows, columns   | Reader token |
| `/data/v1`  | Ingestion      | Append rows, upsert metadata   | Writer token |
| `/setup/v1` | Resource admin | CRUD channels, clone, delete   | Admin token  |
| `/rbac/v1`  | Identity admin | Users, groups, roles, policies | Admin token  |
| `/auth/v1`  | Authentication | Login, change password         | Public       |
| `/health`   | Health         | Liveness, readiness, version   | Public       |

Interactive API documentation is available at `/docs` (Swagger UI) when the server is running.

Full OpenAPI spec: [`docs/openapi.json`](docs/openapi.json)

## SDK

A Python SDK is available for programmatic access:

```sh
python3.12 -m venv .venv
source .venv/bin/activate
pip install kronicle-sdk
```

See the [kronicle-sdk on PyPI](https://pypi.org/project/kronicle-sdk/) for API reference and usage examples.

## Prerequisites (local development)

### PostgreSQL 17 + TimescaleDB (macOS)

```sh
brew install postgresql@17 timescaledb
brew services start postgresql@17

# Add to ~/.zshrc
export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"
export PGDATA="/opt/homebrew/var/postgresql@17"
```

Enable TimescaleDB:

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
```

### Testing

```sh
# Unit tests
pytest

# Integration tests (requires running server)
source .conf/.integration.env && pytest -m integration

# Both
source .conf/.integration.env && pytest --run-all
```

See [README.pynb §6](README.pynb) for a full dev testing walkthrough.

## License

[AGPL-3.0-or-later](LICENCE.md)
