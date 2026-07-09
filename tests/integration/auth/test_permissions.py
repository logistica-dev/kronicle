# tests/integration/auth/test_permissions.py
from collections.abc import Generator

import pytest
import requests as req
from kronicle_sdk.conf.read_conf import ConnectionInformation, Settings
from kronicle_sdk.connectors.channel.channel_setup import KronicleSetup
from kronicle_sdk.connectors.rbac.core_setup import KronicleCore
from kronicle_sdk.connectors.rbac.rbac_setup import KronicleRbac
from kronicle_sdk.models.data.kronicle_channel import KronicleChannel
from kronicle_sdk.models.rbac.kronicle_zone import KronicleZone
from kronicle_sdk.utils.str_utils import slash_join, tiny_id, uuid4_str

pytestmark = pytest.mark.integration

# ==============================================================================
# Helpers
# ==============================================================================


def _jwt(url, login, password):
    r = req.post(slash_join(url, "auth/v1/login"), json={"login": login, "password": password}, timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]


def _get(url, jwt, route):
    return req.get(slash_join(url, route), headers={"Authorization": f"Bearer {jwt}"}, timeout=10)


def _post(url, jwt, route, json=None, params=None):
    return req.post(
        slash_join(url, route), json=json, params=params, headers={"Authorization": f"Bearer {jwt}"}, timeout=10
    )


def _put(url, jwt, route, json=None, params=None):
    return req.put(
        slash_join(url, route), json=json, params=params, headers={"Authorization": f"Bearer {jwt}"}, timeout=10
    )


# ==============================================================================
# Session-level fixtures
# ==============================================================================


@pytest.fixture(scope="session")
def base_url():
    co = Settings().connection_su
    assert co
    return co.url


@pytest.fixture(scope="session")
def su_client():
    """SU-based connector for identity operations."""
    co = Settings().connection_su
    assert co
    return KronicleRbac.from_connection_info(co)


@pytest.fixture(scope="session")
def su_setup_client():
    """SU-based connector for data channel operations."""
    co = Settings().connection_su
    assert co
    return KronicleSetup.from_connection_info(co)


@pytest.fixture(scope="session")
def su_core_client():
    """SU-based connector for core operations (zones)."""
    co = Settings().connection_su
    assert co
    return KronicleCore.from_connection_info(co)


# ==============================================================================
# Test fixtures: roles, user, group, channel
# ==============================================================================


@pytest.fixture(scope="module")
def role_data_reader(su_client) -> Generator[dict, None, None]:
    tag = tiny_id()
    role = su_client.post(
        "/roles",
        {
            "name": f"test_data_reader_{tag}",
            "description": "Can read channel data",
            "permissions": ["channel:read", "rbac:access", "setup:access"],
            "details": {"test": True},
        },
    )
    yield role
    try:
        su_client.delete(f"/roles/{role['id']}")
    except Exception:
        pass


@pytest.fixture(scope="module")
def role_data_writer(su_client) -> Generator[dict, None, None]:
    tag = tiny_id()
    role = su_client.post(
        "/roles",
        {
            "name": f"test_data_writer_{tag}",
            "description": "Can write channel data",
            "permissions": ["row:create", "data:access"],
            "details": {"test": True},
        },
    )
    yield role
    try:
        su_client.delete(f"/roles/{role['id']}")
    except Exception:
        pass


@pytest.fixture(scope="module")
def role_channel_admin(su_client) -> Generator[dict, None, None]:
    tag = tiny_id()
    role = su_client.post(
        "/roles",
        {
            "name": f"test_channel_admin_{tag}",
            "description": "Can create/update/delete channels",
            "permissions": ["channel:create", "channel:update", "channel:delete"],
            "details": {"test": True},
        },
    )
    yield role
    try:
        su_client.delete(f"/roles/{role['id']}")
    except Exception:
        pass


@pytest.fixture(scope="module")
def test_group(su_client, role_channel_admin) -> Generator[dict, None, None]:
    tag = tiny_id()
    group = su_client.post(
        "/groups",
        {
            "name": f"perm_test_group_{tag}",
            "details": {"test": "permissions"},
        },
    )
    su_client.put(f"/groups/{group['id']}/roles/{role_channel_admin['id']}")
    yield group
    try:
        su_client.delete(f"/groups/{group['id']}")
    except Exception:
        pass


@pytest.fixture(scope="module")
def test_user(
    su_client,
    role_data_reader,
    role_data_writer,
    test_group,
) -> Generator[dict, None, None]:
    tag = tiny_id()
    email = f"perm_test_{tag}@kronicle.app"
    password = "PermTest_789!"
    user = su_client.post(
        "/users",
        {
            "email": email,
            "name": f"perm_test_user_{tag}",
            "password": password,
            "details": {"test": True},
        },
    )
    su_client.put(f"/users/{user['id']}/roles/{role_data_reader['id']}")
    su_client.put(f"/users/{user['id']}/roles/{role_data_writer['id']}")
    su_client.post(f"/groups/{test_group['id']}/users", params={"user_id": user["id"]})
    yield {**user, "_password": password, "_email": email}
    try:
        su_client.delete(f"/users/{user['id']}?remove=true")
    except Exception:
        su_client.delete(f"/users/{user['id']}")


@pytest.fixture(scope="module")
def test_zone(su_core_client) -> Generator[str, None, None]:
    tag = tiny_id()
    zone = su_core_client.create_zone(KronicleZone(name=f"perm_test_zone_{tag}", details={"test": True}))
    yield str(zone.id)
    try:
        su_core_client.delete_zone(zone_id=zone.id)
    except Exception:
        pass


@pytest.fixture(scope="module")
def test_channel(su_setup_client, test_zone) -> Generator[str, None, None]:
    channel_id = uuid4_str()
    payload = KronicleChannel.from_json(
        {
            "id": channel_id,
            "name": f"perm_test_chan_{tiny_id()}",
            "channel_schema": {"time": "datetime", "temp": "float"},
            "metadata": {"source": "perm-test"},
            "tags": {"test": "true"},
            "rows": [{"time": "2025-01-10T00:00:00Z", "temp": 22.5}],
        }
    )
    su_setup_client.create_channel(payload, zone_id=test_zone)
    yield channel_id
    try:
        su_setup_client.delete_channel(channel_id)
    except Exception:
        pass


# ==============================================================================
# Test user login
# ==============================================================================


@pytest.fixture(scope="module")
def user_jwt(base_url, test_user):
    return _jwt(base_url, test_user["_email"], test_user["_password"])


# ==============================================================================
# Direct role (channel:read) — positive
# ==============================================================================


class TestDirectRolePermission:
    """User has channel:read via direct user-role assignment."""

    def test_read_channels(self, base_url, user_jwt, test_channel):
        resp = _get(base_url, user_jwt, "/setup/v1/channels")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_read_channel_rows(self, base_url, user_jwt, test_channel):
        resp = _get(base_url, user_jwt, f"/setup/v1/channels/{test_channel}/rows")
        assert resp.status_code == 200

    def test_read_core_channels(self, base_url, user_jwt, test_channel):
        resp = _get(base_url, user_jwt, "/core/v1/channels")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


# ==============================================================================
# Direct role (row:create) — positive
# ==============================================================================


class TestDirectRoleWritePermission:
    """User has row:create via direct user-role assignment."""

    def test_write_rows(self, base_url, user_jwt, test_channel):
        resp = _post(
            base_url,
            user_jwt,
            f"/data/v1/channels/{test_channel}/rows",
            json={
                "id": test_channel,
                "channel_schema": {"time": "datetime", "temp": "float"},
                "rows": [{"time": "2025-06-01T00:00:00Z", "temp": 18.5}],
            },
        )
        assert resp.status_code == 200


# ==============================================================================
# Group role (channel:create inherited via group) — positive
# ==============================================================================


class TestGroupRolePermission:
    """User inherits channel:create via group membership."""

    def test_create_channel(self, base_url, user_jwt, test_zone):
        channel_id = uuid4_str()
        resp = _post(
            base_url,
            user_jwt,
            f"/setup/v1/zones/{test_zone}/channels",
            json={
                "id": channel_id,
                "name": f"group_perm_test_{tiny_id()}",
                "channel_schema": {"time": "datetime", "val": "float"},
                "metadata": {"test": True},
            },
        )
        assert resp.status_code == 200
        connection_su = Settings().connection_su
        assert isinstance(connection_su, ConnectionInformation)
        su = KronicleSetup.from_connection_info(connection_su)
        try:
            su.delete_channel(channel_id)
        except Exception:
            pass


# ==============================================================================
# Missing permissions → 403
# ==============================================================================


class TestMissingPermissionDenied:
    """User lacks user:create, zone:create, role:assign."""

    def test_cannot_create_user(self, base_url, user_jwt):
        tag = tiny_id()
        resp = _post(
            base_url,
            user_jwt,
            "/rbac/v1/users",
            json={
                "email": f"should_fail_{tag}@kronicle.app",
                "name": f"fail_{tag}",
                "password": "ShouldFail_123!",
                "details": {"test": True},
            },
        )
        assert resp.status_code == 403

    def test_cannot_create_zone(self, base_url, user_jwt):
        tag = tiny_id()
        resp = _post(
            base_url,
            user_jwt,
            "/core/v1/zones",
            json={"name": f"should_fail_zone_{tag}"},
        )
        assert resp.status_code == 403

    def test_cannot_assign_role(self, base_url, user_jwt, role_data_reader):
        resp = _put(
            base_url,
            user_jwt,
            f"/rbac/v1/users/00000000-0000-0000-0000-000000000000/roles/{role_data_reader['id']}",
        )
        assert resp.status_code == 403
