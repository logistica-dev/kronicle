# tests/integration/rbac/test_rbac_api_introspect.py
"""Integration tests for RBAC introspection endpoints.

Requires a running server with valid KRONICLE_SU_NAME / KRONICLE_SU_PASS env vars.
"""

import uuid

import pytest
import requests
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.utils.str_utils import slash_join

pytestmark = pytest.mark.integration


# ==============================================================================
# Helpers
# ==============================================================================


def _jwt(url, login, password):
    r = requests.post(slash_join(url, "auth/v1/login"), json={"login": login, "password": password}, timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]


def _get(url, jwt, route, params=None):
    return requests.get(
        slash_join(url, route),
        headers={"Authorization": f"Bearer {jwt}"},
        params=params,
        timeout=10,
    )


# ==============================================================================
# Fixtures
# ==============================================================================


@pytest.fixture(scope="module")
def base_url():
    co = Settings().connection_su
    assert co
    return co.url


@pytest.fixture(scope="module")
def su_jwt(base_url):
    co = Settings().connection_su
    assert co
    return _jwt(base_url, co.usr, co.pwd)


@pytest.fixture(scope="module")
def sample_user_id(test_user):
    """A user guaranteed to exist so introspection endpoints have something to resolve."""
    return test_user.id


@pytest.fixture(scope="module")
def sample_group_id(test_group):
    """A group guaranteed to exist so introspection endpoints have something to resolve."""
    return test_group.id


@pytest.fixture(scope="module")
def sample_zone_id(test_zone):
    """A zone guaranteed to exist so introspection endpoints have something to resolve."""
    return test_zone.id


@pytest.fixture(scope="module")
def sample_channel_id(test_channel):
    """A channel guaranteed to exist so introspection endpoints have something to resolve."""
    return test_channel.id


# ==============================================================================
# User introspection
# ==============================================================================


class TestApiUserPermissions:
    def test_api_get_user_permissions(self, base_url, su_jwt, sample_user_id):
        resp = _get(base_url, su_jwt, f"rbac/v1/users/{sample_user_id}/permissions")
        assert resp.status_code == 200
        body = resp.json()
        assert "roles" in body
        assert "indirect_roles" in body
        assert "zone_policies" in body
        assert "channel_policies" in body
        assert "row_policies" in body
        assert isinstance(body["roles"], list)
        assert isinstance(body["indirect_roles"], list)


class TestApiUserZones:
    def test_api_get_user_zones(self, base_url, su_jwt, sample_user_id):
        resp = _get(base_url, su_jwt, f"rbac/v1/users/{sample_user_id}/zones")
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)

    def test_api_get_user_zones_indirect(self, base_url, su_jwt, sample_user_id):
        resp = _get(base_url, su_jwt, f"rbac/v1/users/{sample_user_id}/zones", params={"indirect": "true"})
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


class TestApiUserChannels:
    def test_api_get_user_channels(self, base_url, su_jwt, sample_user_id):
        resp = _get(base_url, su_jwt, f"rbac/v1/users/{sample_user_id}/channels")
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)
        for entry in body:
            assert "resource" in entry
            assert "policy" in entry

    def test_api_get_user_channels_indirect(self, base_url, su_jwt, sample_user_id):
        resp = _get(base_url, su_jwt, f"rbac/v1/users/{sample_user_id}/channels", params={"indirect": "true"})
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


class TestApiUserRows:
    def test_api_get_user_rows(self, base_url, su_jwt, sample_user_id):
        resp = _get(base_url, su_jwt, f"rbac/v1/users/{sample_user_id}/rows")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


class TestApiUserResources:
    def test_api_get_user_resources(self, base_url, su_jwt, sample_user_id):
        resp = _get(base_url, su_jwt, f"rbac/v1/users/{sample_user_id}/resources")
        assert resp.status_code == 200
        body = resp.json()
        assert "zones" in body
        assert "channels" in body
        assert "rows" in body
        assert isinstance(body["zones"], list)
        assert isinstance(body["channels"], list)
        assert isinstance(body["rows"], list)


# ==============================================================================
# Group introspection
# ==============================================================================


class TestApiGroupPermissions:
    def test_api_get_group_permissions(self, base_url, su_jwt, sample_group_id):
        resp = _get(base_url, su_jwt, f"rbac/v1/groups/{sample_group_id}/permissions")
        assert resp.status_code == 200
        body = resp.json()
        assert "roles" in body
        assert "indirect_roles" in body
        assert "zone_policies" in body


class TestApiGroupZones:
    def test_api_get_group_zones(self, base_url, su_jwt, sample_group_id):
        resp = _get(base_url, su_jwt, f"rbac/v1/groups/{sample_group_id}/zones")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


class TestApiGroupChannels:
    def test_api_get_group_channels(self, base_url, su_jwt, sample_group_id):
        resp = _get(base_url, su_jwt, f"rbac/v1/groups/{sample_group_id}/channels")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


class TestApiGroupRows:
    def test_api_get_group_rows(self, base_url, su_jwt, sample_group_id):
        resp = _get(base_url, su_jwt, f"rbac/v1/groups/{sample_group_id}/rows")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


class TestApiGroupResources:
    def test_api_get_group_resources(self, base_url, su_jwt, sample_group_id):
        resp = _get(base_url, su_jwt, f"rbac/v1/groups/{sample_group_id}/resources")
        assert resp.status_code == 200
        body = resp.json()
        assert "zones" in body
        assert "channels" in body
        assert "rows" in body


# ==============================================================================
# Resource-level policies and access profiles
# ==============================================================================


class TestApiZonePolicies:
    def test_api_list_zone_policies(self, base_url, su_jwt, sample_zone_id):
        resp = _get(base_url, su_jwt, f"rbac/v1/zones/{sample_zone_id}/policies")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_api_list_zone_access_profiles(self, base_url, su_jwt, sample_zone_id):
        resp = _get(base_url, su_jwt, f"rbac/v1/zones/{sample_zone_id}/access_profiles")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


class TestApiChannelPolicies:
    def test_api_list_channel_policies(self, base_url, su_jwt, sample_channel_id):
        resp = _get(base_url, su_jwt, f"rbac/v1/channels/{sample_channel_id}/policies")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_api_list_channel_access_profiles(self, base_url, su_jwt, sample_channel_id):
        resp = _get(base_url, su_jwt, f"rbac/v1/channels/{sample_channel_id}/access_profiles")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


class TestApiRowPolicies:
    def test_api_list_row_policies_nonexistent(self, base_url, su_jwt):
        """Row policies on a nonexistent row should return 404 or empty list."""
        fake_row_id = str(uuid.uuid4())
        resp = _get(base_url, su_jwt, f"rbac/v1/rows/{fake_row_id}/policies")
        assert resp.status_code in [200, 404]

    def test_api_list_row_access_profiles_nonexistent(self, base_url, su_jwt):
        """Access profiles on a nonexistent row should return 404 or empty list."""
        fake_row_id = str(uuid.uuid4())
        resp = _get(base_url, su_jwt, f"rbac/v1/rows/{fake_row_id}/access_profiles")
        assert resp.status_code in [200, 404]


# ==============================================================================
# Unauthorized access
# ==============================================================================


class TestIntrospectUnauthorized:
    """Anonymous requests to introspection endpoints should be rejected."""

    def test_api_user_permissions_requires_auth(self, base_url):
        fake_id = str(uuid.uuid4())
        resp = requests.get(f"{base_url}/rbac/v1/users/{fake_id}/permissions", timeout=5)
        assert resp.status_code in [401, 403]

    def test_api_user_zones_requires_auth(self, base_url):
        fake_id = str(uuid.uuid4())
        resp = requests.get(f"{base_url}/rbac/v1/users/{fake_id}/zones", timeout=5)
        assert resp.status_code in [401, 403]

    def test_api_group_permissions_requires_auth(self, base_url):
        fake_id = str(uuid.uuid4())
        resp = requests.get(f"{base_url}/rbac/v1/groups/{fake_id}/permissions", timeout=5)
        assert resp.status_code in [401, 403]

    def test_api_zone_policies_requires_auth(self, base_url):
        fake_id = str(uuid.uuid4())
        resp = requests.get(f"{base_url}/rbac/v1/zones/{fake_id}/policies", timeout=5)
        assert resp.status_code in [401, 403]
