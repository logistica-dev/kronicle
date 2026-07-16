# Kronicle

FastAPI × TimescaleDB microservice for storing time-series measurements.

Kronicle organises data into **channels** (named streams with user-defined schemas), stores rows in append-only TimescaleDB hypertables, and enforces access control through a role-based system with **zones** as isolation boundaries.

## Key Concepts

| Concept | What it is |
|---------|-----------|
| **Channel** | A named stream of time-series data with a user-defined schema. Each channel maps to its own TimescaleDB hypertable (`channel_{uuid}`). |
| **Schema** | Column definitions as `{name: type}`. Supported types: `str`, `int`, `float`, `bool`, `uuid`, `datetime`, `dict`, `list`. Types can be wrapped with `optional[...]`. |
| **Zone** | A workspace or project boundary. Zones act as RBAC domains — permissions assigned at the zone level apply to all channels created inside it. |
| **Row** | A single data point in a channel. Rows are append-only once inserted. |

## Quick Start

The fastest way to get started is the interactive notebook: **[README.pynb](README.pynb)**

It walks through pulling the image, running the server, creating channels, writing data, and setting up RBAC — all with executable code cells.

## Installation

### Docker (recommended)

```sh
podman pull ghcr.io/logistica-dev/kronicle:latest
```

Create a `.env` file (see [Quick Start notebook](README.pynb) for the minimum required variables), then:

```sh
podman-compose up -d
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

| Variable | Purpose | Default |
|----------|---------|---------|
| `POSTGRES_USER` | DB superuser name | — (required at init) |
| `POSTGRES_PASSWORD` | DB superuser password | — (required at init) |
| `KRONICLE_DB_NAME` | Database name | `kronicle_db` |
| `KRONICLE_SU_INFO` | Superuser credentials (base64url-encoded `name:email:argon2_hash`) | — (required at init) |
| `KRONICLE_CHAN_CREDS` | Channel DB user (`base64url user:pass`) | — (required) |
| `KRONICLE_RBAC_CREDS` | RBAC DB user (`base64url user:pass`) | — (required) |
| `KRONICLE_PORT` | Server port | `8000` |
| `KRONICLE_HOST` | Server bind address | `0.0.0.0` |
| `KRONICLE_LOG_LEVEL` | 0=error, 1=warn, 2=info, 3=debug | `2` |

Full configuration reference: [`conf/default-conf.ini`](conf/default-conf.ini)

Generate the superuser credentials string:

```sh
python3 ./scripts/utils/hash_creds.py su_name su_email "SU_passw0rd"
```

## API at a Glance

The API is split into four route groups (security lanes):

| Prefix | Lane | Purpose | Auth |
|--------|------|---------|------|
| `/api/v1` | Consumption | Read channels, rows, columns | Reader token |
| `/data/v1` | Ingestion | Append rows, upsert metadata | Writer token |
| `/setup/v1` | Resource admin | CRUD channels, clone, delete | Admin token |
| `/rbac/v1` | Identity admin | Users, groups, roles, policies | Admin token |
| `/auth/v1` | Authentication | Login, change password | Public |
| `/health` | Health | Liveness, readiness, version | Public |

Interactive API documentation is available at `/docs` (Swagger UI) when the server is running.

Full OpenAPI spec: [`docs/openapi.json`](docs/openapi.json)

## SDK

A Python SDK is available for programmatic access:

```
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
