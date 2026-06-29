# tests/integration/auth/test_default_role_permissions.py
"""Verify each default role can only access its permitted routes.

Each test class:
  1. Looks up a seeded default role by name on the running server.
  2. Creates a test user and assigns that role.
  3. Logs in as that user.
  4. Tests expected-allowed endpoints (200) and expected-denied (403).

Shared resources (zone, channel) are created once per module by a superuser.
"""

from __future__ import annotations

from collections.abc import Generator

import pytest
import requests as req
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.channel.channel_setup import KronicleSetup
from kronicle_sdk.connectors.rbac.core_setup import KronicleCore
from kronicle_sdk.connectors.rbac.rbac_setup import KronicleRbac
from kronicle_sdk.models.rbac.kronicle_zone import KronicleZone
from kronicle_sdk.utils.str_utils import slash_join, tiny_id, uuid4_str

pytestmark = pytest.mark.integration


# ==============================================================================
# Helpers
# ==============================================================================


def _jwt(url: str, login: str, password: str) -> str:
    r = req.post(slash_join(url, "auth/v1/login"), json={"login": login, "password": password}, timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]


def _get(url: str, jwt: str, route: str) -> req.Response:
    return req.get(slash_join(url, route), headers={"Authorization": f"Bearer {jwt}"}, timeout=10)


def _post(url: str, jwt: str, route: str, json: dict | None = None, params: dict | None = None) -> req.Response:
    return req.post(
        slash_join(url, route), json=json, params=params, headers={"Authorization": f"Bearer {jwt}"}, timeout=10
    )


def _delete(url: str, jwt: str, route: str) -> req.Response:
    return req.delete(slash_join(url, route), headers={"Authorization": f"Bearer {jwt}"}, timeout=10)


def _create_user(su_client: KronicleRbac, email: str, password: str, name: str) -> dict:
    return su_client.post("/users", {"email": email, "name": name, "password": password, "details": {"test": True}})


def _cleanup_user(su_client: KronicleRbac, user_id: str) -> None:
    try:
        su_client.delete(f"/users/{user_id}?remove=true")
    except Exception:
        try:
            su_client.delete(f"/users/{user_id}")
        except Exception:
            pass


def _resolve_role_id(su_client: KronicleRbac, role_name: str) -> str:
    roles = su_client.get("/roles")
    return next(r["id"] for r in roles if r["name"] == role_name)


def _role_user_fixture(su_client: KronicleRbac, role_name: str, tag: str) -> Generator[dict, None, None]:
    email = f"{role_name}_{tag}@kronicle.app"
    password = "RoleTest_123!"
    user = _create_user(su_client, email, password, f"{role_name}_user_{tag}")
    role_id = _resolve_role_id(su_client, role_name)
    su_client.put(f"/users/{user['id']}/roles/{role_id}")
    yield {**user, "_password": password, "_email": email}
    _cleanup_user(su_client, user["id"])


# ==============================================================================
# Session-level fixtures
# ==============================================================================


@pytest.fixture(scope="session")
def base_url() -> str:
    co = Settings().connection_su
    assert co
    return co.url


@pytest.fixture(scope="session")
def su_client() -> KronicleRbac:
    co = Settings().connection_su
    assert co
    return KronicleRbac.from_connection_info(co)


@pytest.fixture(scope="session")
def su_jwt() -> str:
    co = Settings().connection_su
    assert co
    return _jwt(co.url, co.usr, co.pwd)


@pytest.fixture(scope="session")
def su_setup_client() -> KronicleSetup:
    co = Settings().connection_su
    assert co
    return KronicleSetup.from_connection_info(co)


@pytest.fixture(scope="session")
def su_core_client() -> KronicleCore:
    co = Settings().connection_su
    assert co
    return KronicleCore.from_connection_info(co)


# ==============================================================================
# Shared resources (zone + channel) – created once per module via superuser
# ==============================================================================


@pytest.fixture(scope="module")
def test_zone(su_core_client) -> Generator[str, None, None]:
    tag = tiny_id()
    zone = su_core_client.create_zone(KronicleZone(name=f"default_role_zone_{tag}", details={"test": True}))
    yield str(zone.id)
    try:
        su_core_client.delete_zone(zone_id=zone.id)
    except Exception:
        pass


@pytest.fixture(scope="module")
def test_channel(su_setup_client, test_zone) -> Generator[str, None, None]:
    channel_id = uuid4_str()
    payload = {
        "channel_id": channel_id,
        "name": f"default_role_chan_{tiny_id()}",
        "channel_schema": {"time": "datetime", "val": "float"},
        "metadata": {"source": "default-role-test"},
        "tags": {"test": "true"},
        "rows": [{"time": "2025-01-01T00:00:00Z", "val": 1.0}],
    }
    su_setup_client.create_channel(payload, zone_id=test_zone)
    yield channel_id
    try:
        su_setup_client.delete_channel(channel_id)
    except Exception:
        pass


# ==============================================================================
# SUPER_ADMIN – all routes allowed
# ==============================================================================


class TestSuperAdmin:
    """super_admin has every permission – all requests should return 200."""

    @pytest.fixture(scope="class")
    def test_user(self, su_client) -> Generator[dict, None, None]:
        yield from _role_user_fixture(su_client, "super_admin", tiny_id())

    @pytest.fixture(scope="class")
    def jwt(self, base_url, test_user) -> str:
        return _jwt(base_url, test_user["_email"], test_user["_password"])

    def test_create_user(self, su_client, base_url, jwt):
        tag = tiny_id()
        r = _post(
            base_url,
            jwt,
            "/rbac/v1/users",
            json={
                "email": f"su_sub_{tag}@kronicle.app",
                "name": f"su_sub_{tag}",
                "password": "SubUser_123!",
                "details": {"test": True},
            },
        )
        assert r.status_code == 200
        su_client.delete(f"/users/{r.json()['id']}?remove=true")

    def test_create_role(self, su_client, base_url, jwt):
        tag = tiny_id()
        r = _post(
            base_url,
            jwt,
            "/rbac/v1/roles",
            json={"name": f"su_role_{tag}", "permissions": ["channel:read"], "details": {"test": True}},
        )
        assert r.status_code == 200
        try:
            su_client.delete(f"/roles/{r.json()['id']}")
        except Exception:
            pass

    def test_create_group(self, su_client, base_url, jwt):
        tag = tiny_id()
        r = _post(base_url, jwt, "/rbac/v1/groups", json={"name": f"su_group_{tag}", "details": {"test": True}})
        assert r.status_code == 200
        try:
            su_client.delete(f"/groups/{r.json()['id']}")
        except Exception:
            pass

    def test_create_zone(self, base_url, su_core_client, jwt):
        tag = tiny_id()
        r = _post(base_url, jwt, "/core/v1/zones", json={"name": f"su_zone_{tag}", "details": {"test": True}})
        assert r.status_code == 200
        try:
            su_core_client.delete_zone(zone_id=r.json()["id"])
        except Exception:
            pass

    def test_create_channel(self, base_url, jwt, su_setup_client):
        cid = uuid4_str()
        r = _post(
            base_url,
            jwt,
            "/setup/v1/channels",
            json={
                "channel_id": cid,
                "name": f"su_chan_{tiny_id()}",
                "channel_schema": {"time": "datetime", "val": "float"},
                "tags": {"test": True},
                "metadata": {"test": True},
            },
        )
        assert r.status_code == 200, f"Body: {r.text}"
        try:
            su_setup_client.delete_channel(cid)
        except Exception:
            pass

    def test_write_rows(self, base_url, jwt, test_channel):
        r = _post(
            base_url,
            jwt,
            f"/data/v1/channels/{test_channel}/rows",
            json={
                "channel_id": test_channel,
                "channel_schema": {"time": "datetime", "val": "float"},
                "tags": {"test": True},
                "rows": [{"time": "2025-06-01T00:00:00Z", "val": 42.0}],
            },
        )
        assert r.status_code == 200


# ==============================================================================
# RBAC_ADMIN – only RBAC operations allowed
# ==============================================================================


class TestRbacAdmin:
    """rbac_admin has RBAC_ACCESS + USER/ROLE/GROUP/POLICY CRUD."""

    @pytest.fixture(scope="class")
    def test_user(self, su_client) -> Generator[dict, None, None]:
        yield from _role_user_fixture(su_client, "rbac_admin", tiny_id())

    @pytest.fixture(scope="class")
    def jwt(self, base_url, test_user) -> str:
        return _jwt(base_url, test_user["_email"], test_user["_password"])

    # Allowed
    def test_create_user(self, su_client, base_url, jwt):
        tag = tiny_id()
        r = _post(
            base_url,
            jwt,
            "/rbac/v1/users",
            json={
                "email": f"rbac_sub_{tag}@kronicle.app",
                "name": f"rbac_sub_{tag}",
                "password": "SubUser_123!",
                "details": {"test": True},
            },
        )
        assert r.status_code == 200
        try:
            su_client.delete(f"/users/{r.json()['id']}?remove=true")
        except Exception:
            pass

    def test_create_role(self, su_client, base_url, jwt):
        tag = tiny_id()
        r = _post(
            base_url,
            jwt,
            "/rbac/v1/roles",
            json={
                "name": f"rbac_role_{tag}",
                "permissions": ["channel:read"],
                "details": {"test": True},
            },
        )
        assert r.status_code == 200
        try:
            su_client.delete(f"/roles/{r.json()['id']}")
        except Exception:
            pass

    def test_create_group(self, su_client, base_url, jwt):
        tag = tiny_id()
        r = _post(base_url, jwt, "/rbac/v1/groups", json={"name": f"rbac_group_{tag}", "details": {"test": True}})
        assert r.status_code == 200
        try:
            su_client.delete(f"/groups/{r.json()['id']}")
        except Exception:
            pass

    def test_list_roles(self, base_url, jwt):
        r = _get(base_url, jwt, "/rbac/v1/roles")
        assert r.status_code == 200

    def test_list_users(self, base_url, jwt):
        r = _get(base_url, jwt, "/rbac/v1/users")
        assert r.status_code == 200

    # Denied
    def test_cannot_create_zone(self, base_url, jwt):
        r = _post(base_url, jwt, "/core/v1/zones", json={"name": "should_fail"})
        assert r.status_code == 403

    def test_cannot_create_channel(self, base_url, jwt):
        r = _post(
            base_url,
            jwt,
            "/setup/v1/channels",
            json={
                "channel_id": uuid4_str(),
                "name": "should_fail",
                "tags": {"test": True},
                "channel_schema": {"x": "int"},
            },
        )
        assert r.status_code == 403

    def test_cannot_write_rows(self, base_url, jwt, test_channel):
        r = _post(
            base_url,
            jwt,
            f"/data/v1/channels/{test_channel}/rows",
            json={
                "channel_id": test_channel,
                "channel_schema": {"x": "int"},
                "tags": {"test": True},
                "rows": [],
            },
        )
        assert r.status_code == 403


# ==============================================================================
# DATA_READER – only CHANNEL_READ + ROW_READ, no router-level gates
# ==============================================================================


class TestDataReader:
    """data_reader has CHANNEL_READ + ROW_READ but no SETUP_ACCESS, DATA_ACCESS, or RBAC_ACCESS.

    Can only access core endpoints (core_router has no gate beyond require_auth).
    """

    @pytest.fixture(scope="class")
    def test_user(self, su_client) -> Generator[dict, None, None]:
        yield from _role_user_fixture(su_client, "data_reader", tiny_id())

    @pytest.fixture(scope="class")
    def jwt(self, base_url, test_user) -> str:
        return _jwt(base_url, test_user["_email"], test_user["_password"])

    # Allowed via core_router (no gate, just CHANNEL_READ)
    def test_read_core_channels(self, base_url, jwt):
        r = _get(base_url, jwt, "/core/v1/channels")
        assert r.status_code == 200

    def test_read_core_channel_by_id(self, base_url, jwt, test_channel):
        r = _get(base_url, jwt, f"/core/v1/channels/{test_channel}")
        assert r.status_code == 200

    # Denied – no SETUP_ACCESS on setup_router
    def test_cannot_list_setup_channels(self, base_url, jwt):
        r = _get(base_url, jwt, "/setup/v1/channels")
        assert r.status_code == 403

    def test_cannot_read_setup_rows(self, base_url, jwt, test_channel):
        r = _get(base_url, jwt, f"/setup/v1/channels/{test_channel}/rows")
        assert r.status_code == 403

    # Denied – no RBAC_ACCESS on rbac_router
    def test_cannot_list_rbac_users(self, base_url, jwt):
        r = _get(base_url, jwt, "/rbac/v1/users")
        assert r.status_code == 403

    # Denied – no perms for mutation
    def test_cannot_create_zone(self, base_url, jwt):
        r = _post(base_url, jwt, "/core/v1/zones", json={"name": "should_fail"})
        assert r.status_code == 403

    def test_cannot_write_rows(self, base_url, jwt, test_channel):
        r = _post(
            base_url,
            jwt,
            f"/data/v1/channels/{test_channel}/rows",
            json={
                "channel_id": test_channel,
                "channel_schema": {"x": "int"},
                "tags": {"test": True},
                "rows": [],
            },
        )
        assert r.status_code == 403

    def test_cannot_create_channel(self, base_url, jwt):
        r = _post(
            base_url,
            jwt,
            "/setup/v1/channels",
            json={
                "channel_id": uuid4_str(),
                "name": "should_fail",
                "channel_schema": {"x": "int"},
                "tags": {"test": True},
            },
        )
        assert r.status_code == 403


# ==============================================================================
# DATA_WRITER – write rows + read channels, no RBAC/setup gate
# ==============================================================================


class TestDataWriter:
    """data_writer has DATA_ACCESS + CHANNEL_READ + ROW_CREATE.

    Can access writer_router (requires DATA_ACCESS) and core_router.
    """

    @pytest.fixture(scope="class")
    def test_user(self, su_client) -> Generator[dict, None, None]:
        yield from _role_user_fixture(su_client, "data_writer", tiny_id())

    @pytest.fixture(scope="class")
    def jwt(self, base_url, test_user) -> str:
        return _jwt(base_url, test_user["_email"], test_user["_password"])

    # Allowed – core_router (no gate)
    def test_read_core_channels(self, base_url, jwt):
        r = _get(base_url, jwt, "/core/v1/channels")
        assert r.status_code == 200

    # Allowed – writer_router (DATA_ACCESS) + shared_writer_router (ROW_CREATE)
    def test_write_rows(self, base_url, jwt, test_channel):
        r = _post(
            base_url,
            jwt,
            f"/data/v1/channels/{test_channel}/rows",
            json={
                "channel_id": test_channel,
                "channel_schema": {"time": "datetime", "val": "float"},
                "tags": {"test": True},
                "rows": [{"time": "2025-06-01T00:00:00Z", "val": 99.0}],
            },
        )
        assert r.status_code == 200

    # Denied – no SETUP_ACCESS on setup_router
    def test_cannot_create_channel(self, base_url, jwt):
        r = _post(
            base_url,
            jwt,
            "/setup/v1/channels",
            json={
                "channel_id": uuid4_str(),
                "name": "should_fail",
                "channel_schema": {"x": "int"},
                "tags": {"test": True},
            },
        )
        assert r.status_code == 403

    def test_cannot_delete_channel(self, base_url, jwt, test_channel):
        r = _delete(base_url, jwt, f"/setup/v1/channels/{test_channel}")
        assert r.status_code == 403

    # Denied – no RBAC_ACCESS on rbac_router
    def test_cannot_create_user(self, base_url, jwt):
        tag = tiny_id()
        r = _post(
            base_url,
            jwt,
            "/rbac/v1/users",
            json={"email": f"should_fail_{tag}@kronicle.app", "name": f"fail_{tag}", "password": "Fail_123!"},
        )
        assert r.status_code == 403

    # Denied – no ZONE_CREATE
    def test_cannot_create_zone(self, base_url, jwt):
        r = _post(base_url, jwt, "/core/v1/zones", json={"name": "should_fail"})
        assert r.status_code == 403


# ==============================================================================
# CHANNEL_ADMIN – setup/channel operations allowed
# ==============================================================================


class TestChannelAdmin:
    """channel_admin has SETUP_ACCESS + CHANNEL_CRUD + ROW_DELETE."""

    @pytest.fixture(scope="class")
    def test_user(self, su_client) -> Generator[dict, None, None]:
        yield from _role_user_fixture(su_client, "channel_admin", tiny_id())

    @pytest.fixture(scope="class")
    def jwt(self, base_url, test_user) -> str:
        return _jwt(base_url, test_user["_email"], test_user["_password"])

    # Allowed
    def test_list_channels(self, base_url, jwt):
        r = _get(base_url, jwt, "/setup/v1/channels")
        assert r.status_code == 200

    def test_create_channel(self, base_url, jwt, su_setup_client):
        cid = uuid4_str()
        r = _post(
            base_url,
            jwt,
            "/setup/v1/channels",
            json={
                "channel_id": cid,
                "name": f"ca_chan_{tiny_id()}",
                "channel_schema": {"time": "datetime", "val": "float"},
                "tags": {"test": True},
                "metadata": {"test": True},
            },
        )
        assert r.status_code == 200, f"Body: {r.text}"
        try:
            su_setup_client.delete_channel(cid)
        except Exception:
            pass

    def test_delete_channel(self, base_url, jwt, test_zone, su_client, su_jwt):
        cid = uuid4_str()
        r_create = _post(
            base_url,
            su_jwt,
            "/setup/v1/channels",
            json={
                "channel_id": cid,
                "name": f"ca_del_{tiny_id()}",
                "channel_schema": {"time": "datetime", "val": "float"},
                "tags": {"test": True},
                "metadata": {"test": True},
            },
        )
        assert r_create.status_code == 200, f"Create failed: {r_create.text}"
        r = _delete(base_url, jwt, f"/setup/v1/channels/{cid}")
        assert r.status_code == 200

    # Denied – no RBAC_ACCESS
    def test_cannot_create_user(self, base_url, jwt):
        tag = tiny_id()
        r = _post(
            base_url,
            jwt,
            "/rbac/v1/users",
            json={
                "email": f"should_fail_{tag}@kronicle.app",
                "name": f"fail_{tag}",
                "password": "Fail_123!",
            },
        )
        assert r.status_code == 403

    # Denied – no ZONE_CREATE
    def test_cannot_create_zone(self, base_url, jwt):
        r = _post(base_url, jwt, "/core/v1/zones", json={"name": "should_fail"})
        assert r.status_code == 403

    # Denied – no DATA_ACCESS + no ROW_CREATE
    def test_cannot_write_rows(self, base_url, jwt, test_channel):
        r = _post(
            base_url,
            jwt,
            f"/data/v1/channels/{test_channel}/rows",
            json={
                "channel_id": test_channel,
                "channel_schema": {"x": "int"},
                "tags": {"test": True},
                "rows": [],
            },
        )
        assert r.status_code == 403


# ==============================================================================
# ZONE_ADMIN – zone CRUD allowed only
# ==============================================================================


class TestZoneAdmin:
    """zone_admin has ZONE_CRUD only."""

    @pytest.fixture(scope="class")
    def test_user(self, su_client) -> Generator[dict, None, None]:
        yield from _role_user_fixture(su_client, "zone_admin", tiny_id())

    @pytest.fixture(scope="class")
    def jwt(self, base_url, test_user) -> str:
        return _jwt(base_url, test_user["_email"], test_user["_password"])

    # Allowed
    def test_create_zone(self, su_core_client, base_url, jwt):
        tag = tiny_id()
        r = _post(base_url, jwt, "/core/v1/zones", json={"name": f"za_zone_{tag}", "details": {"test": True}})
        assert r.status_code == 200
        try:
            su_core_client.delete_zone(zone_id=r.json()["id"])
        except Exception:
            pass

    def test_list_zones(self, base_url, jwt):
        r = _get(base_url, jwt, "/core/v1/zones")
        assert r.status_code == 200

    # Denied – no RBAC_ACCESS
    def test_cannot_create_user(self, base_url, jwt):
        tag = tiny_id()
        r = _post(
            base_url,
            jwt,
            "/rbac/v1/users",
            json={"email": f"should_fail_{tag}@kronicle.app", "name": f"fail_{tag}", "password": "Fail_123!"},
        )
        assert r.status_code == 403

    # Denied – no SETUP_ACCESS + no CHANNEL_CREATE
    def test_cannot_create_channel(self, base_url, jwt):
        r = _post(
            base_url,
            jwt,
            "/setup/v1/channels",
            json={"channel_id": uuid4_str(), "name": "should_fail", "channel_schema": {"x": "int"}},
        )
        assert r.status_code == 403

    # Denied – no DATA_ACCESS, no ROW_CREATE
    def test_cannot_write_rows(self, base_url, jwt, test_channel):
        r = _post(
            base_url,
            jwt,
            f"/data/v1/channels/{test_channel}/rows",
            json={"channel_id": test_channel, "channel_schema": {"x": "int"}, "rows": []},
        )
        assert r.status_code == 403


# ==============================================================================
# AUDITOR – read-only across all subsystems (via RBAC_ACCESS + read perms)
# ==============================================================================


class TestAuditor:
    """auditor has RBAC_ACCESS + read permissions on all resources.

    Can access rbac_router (RBAC_ACCESS) for read-only RBAC ops,
    and core_router (no gate) for zone/channel reads.
    """

    @pytest.fixture(scope="class")
    def test_user(self, su_client) -> Generator[dict, None, None]:
        yield from _role_user_fixture(su_client, "auditor", tiny_id())

    @pytest.fixture(scope="class")
    def jwt(self, base_url, test_user) -> str:
        return _jwt(base_url, test_user["_email"], test_user["_password"])

    # Allowed – read RBAC
    def test_list_users(self, base_url, jwt):
        r = _get(base_url, jwt, "/rbac/v1/users")
        assert r.status_code == 200

    def test_list_roles(self, base_url, jwt):
        r = _get(base_url, jwt, "/rbac/v1/roles")
        assert r.status_code == 200

    def test_list_groups(self, base_url, jwt):
        r = _get(base_url, jwt, "/rbac/v1/groups")
        assert r.status_code == 200

    # Allowed – read zones via core_router (no gate)
    def test_list_zones(self, base_url, jwt):
        r = _get(base_url, jwt, "/core/v1/zones")
        assert r.status_code == 200

    # Allowed – read channels via core_router
    def test_read_core_channels(self, base_url, jwt):
        r = _get(base_url, jwt, "/core/v1/channels")
        assert r.status_code == 200

    # Denied – no write perms
    def test_cannot_create_user(self, base_url, jwt):
        tag = tiny_id()
        r = _post(
            base_url,
            jwt,
            "/rbac/v1/users",
            json={"email": f"should_fail_{tag}@kronicle.app", "name": f"fail_{tag}", "password": "Fail_123!"},
        )
        assert r.status_code == 403

    def test_cannot_create_zone(self, base_url, jwt):
        r = _post(base_url, jwt, "/core/v1/zones", json={"name": "should_fail"})
        assert r.status_code == 403

    def test_cannot_create_channel(self, base_url, jwt):
        r = _post(
            base_url,
            jwt,
            "/setup/v1/channels",
            json={"channel_id": uuid4_str(), "name": "should_fail", "channel_schema": {"x": "int"}},
        )
        assert r.status_code == 403

    def test_cannot_write_rows(self, base_url, jwt, test_channel):
        r = _post(
            base_url,
            jwt,
            f"/data/v1/channels/{test_channel}/rows",
            json={"channel_id": test_channel, "channel_schema": {"x": "int"}, "rows": []},
        )
        assert r.status_code == 403

    # Denied – no SETUP_ACCESS / DATA_ACCESS for read endpoints behind gated routers
    def test_cannot_list_setup_channels(self, base_url, jwt):
        r = _get(base_url, jwt, "/setup/v1/channels")
        assert r.status_code == 403

    def test_cannot_read_setup_rows(self, base_url, jwt, test_channel):
        r = _get(base_url, jwt, f"/setup/v1/channels/{test_channel}/rows")
        assert r.status_code == 403
