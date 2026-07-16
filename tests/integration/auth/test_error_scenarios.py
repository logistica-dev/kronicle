# tests/integration/auth/test_error_scenarios.py
"""Integration tests that provoke error responses on purpose."""

import uuid

import pytest
import requests
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.auth.kronicle_auth import KronicleUsrLogin
from kronicle_sdk.connectors.channel.channel_setup import KronicleSetup
from kronicle_sdk.connectors.rbac.core_setup import KronicleCore
from kronicle_sdk.connectors.rbac.rbac_setup import KronicleRbac
from kronicle_sdk.models.rbac.kronicle_group import KronicleGroup
from kronicle_sdk.models.rbac.kronicle_zone import KronicleZone
from kronicle_sdk.utils.str_utils import tiny_id


@pytest.fixture(scope="session")
def base_url():
    co = Settings().connection_su
    assert co
    return co.url


@pytest.fixture(scope="session")
def su_client():
    co = Settings().connection_su
    assert co
    return KronicleUsrLogin.from_connection_info(co)


@pytest.fixture(scope="session")
def rbac(su_client):
    return KronicleRbac(su_client.url, su_client.usr, su_client.pwd)


@pytest.fixture(scope="session")
def rbac_setup(su_client):
    return KronicleCore(su_client.url, su_client.usr, su_client.pwd)


@pytest.fixture(scope="session")
def setup_client(su_client):
    return KronicleSetup(su_client.url, su_client.usr, su_client.pwd)


# ---------------------------------------------------------------------------
# 401 — Unauthorized
# ---------------------------------------------------------------------------


class TestUnauthorized:
    """Provoke 401 responses by sending no/invalid credentials."""

    def test_no_auth_header(self, base_url):
        resp = requests.get(f"{base_url}/rbac/v1/users", timeout=5)
        assert resp.status_code in [401, 403]
        body = resp.json()
        assert body["error"] in ["Forbidden", "Unauthorized"]
        assert (msg := body["message"]) == "Authorization header missing" or msg.startswith(
            "Missing required permission"
        )

    def test_bad_token_format(self, base_url):
        resp = requests.get(
            f"{base_url}/rbac/v1/users",
            headers={"Authorization": "NotBearer xyz"},
            timeout=5,
        )
        assert resp.status_code == 401
        body = resp.json()
        assert body["error"] in "Unauthorized"

    def test_bogus_jwt(self, base_url):
        resp = requests.get(
            f"{base_url}/rbac/v1/users",
            headers={"Authorization": "Bearer this.is.not.a.valid.jwt"},
            timeout=5,
        )
        assert resp.status_code == 401
        body = resp.json()
        assert body["detail"]["error"] == "Unauthorized"

    def test_empty_token(self, base_url):
        resp = requests.get(
            f"{base_url}/rbac/v1/users",
            headers={"Authorization": "Bearer "},
            timeout=5,
        )
        assert resp.status_code == 401
        body = resp.json()
        assert body["error"] == "Unauthorized"

    def test_login_bad_password(self, base_url):
        co = Settings().connection
        resp = requests.post(
            f"{base_url}/auth/v1/login",
            json={
                "login": co.usr,
                "password": "wrong_password_xyz",
                "details": {"test": True},
            },
            timeout=5,
        )
        assert resp.status_code == 401
        body = resp.json()
        assert body["error"] == "Unauthorized"
        assert body["message"] == "Invalid credentials"

    def test_login_nonexistent_user(self, base_url):
        resp = requests.post(
            f"{base_url}/auth/v1/login",
            json={
                "login": "does.not.exist@kronicle.app",
                "password": "SomePass_123",
                "details": {"test": True},
            },
            timeout=5,
        )
        assert resp.status_code == 401
        body = resp.json()
        assert body["error"] == "Unauthorized"
        assert body["message"] == "Invalid credentials"


# ---------------------------------------------------------------------------
# 404 — Not Found
# ---------------------------------------------------------------------------


class TestNotFound:
    """Provoke 404 responses for non-existent resources."""

    def test_get_nonexistent_group(self, rbac):
        fake_id = uuid.uuid4()
        with pytest.raises(Exception) as exc_info:
            rbac.get_group_by_id(group_id=fake_id)
        assert "404" in str(exc_info.value) or "Not Found" in str(exc_info.value)

    def test_get_nonexistent_zone(self, rbac_setup):
        fake_id = uuid.uuid4()
        with pytest.raises(Exception) as exc_info:
            rbac_setup.get_zone_by_id(zone_id=fake_id)
        assert "404" in str(exc_info.value) or "Not Found" in str(exc_info.value)

    def test_get_nonexistent_channel(self, setup_client, base_url):
        fake_id = uuid.uuid4()
        resp = requests.get(
            f"{base_url}/setup/v1/channels/{fake_id}",
            headers={"Authorization": f"Bearer {setup_client.jwt}"},
            timeout=5,
        )
        assert resp.status_code == 404
        body = resp.json()
        assert body["error"] == "NotFound"

    def test_delete_nonexistent_group(self, rbac, base_url):
        fake_id = uuid.uuid4()
        resp = requests.delete(
            f"{base_url}/rbac/v1/groups/{fake_id}",
            headers={"Authorization": f"Bearer {rbac.jwt}"},
            timeout=5,
        )
        assert resp.status_code == 404
        body = resp.json()
        assert body["error"] == "NotFound"

    def test_delete_nonexistent_zone(self, rbac_setup, base_url):
        fake_id = uuid.uuid4()
        resp = requests.delete(
            f"{base_url}/core/v1/zones/{fake_id}",
            headers={"Authorization": f"Bearer {rbac_setup.jwt}"},
            timeout=5,
        )
        assert resp.status_code == 404
        body = resp.json()
        assert body["error"] == "NotFound"


# ---------------------------------------------------------------------------
# 400 — Bad Request (custom validation)
# ---------------------------------------------------------------------------


class TestBadRequest:
    """Provoke 400 responses via custom schema validators."""

    def test_duplicate_group_name(self, rbac):
        tag = tiny_id()
        name = f"dup_group_{tag}"
        group = KronicleGroup(name=name, details={"test": True})
        created = rbac.create_group(group)
        try:
            assert created is not None
            with pytest.raises(Exception) as exc_info:
                rbac.create_group(KronicleGroup(name=name, details={"test": True}))
            assert "400" in str(exc_info.value) or "Bad Request" in str(exc_info.value)
        finally:
            rbac.delete_group(group_id=created.id)

    def test_group_name_too_short(self, rbac, base_url):
        resp = requests.post(
            f"{base_url}/rbac/v1/groups",
            headers={"Authorization": f"Bearer {rbac.jwt}"},
            json={"name": "ab"},
            timeout=5,
        )
        assert resp.status_code == 422
        body = resp.json()
        assert body["error"] == "ValidationError"

    def test_zone_duplicate_name(self, rbac_setup):
        tag = tiny_id()
        name = f"dup_zone_{tag}"
        zone = KronicleZone(name=name, details={"test": True})
        created = rbac_setup.create_zone(zone)
        try:
            assert created is not None
            with pytest.raises(Exception) as exc_info:
                rbac_setup.create_zone(KronicleZone(name=name, details={"test": True}))
            assert "400" in str(exc_info.value) or "Bad Request" in str(exc_info.value)
        finally:
            rbac_setup.delete_zone(zone_id=created.id)


# ---------------------------------------------------------------------------
# 422 — Validation Error (Pydantic type coercion failure)
# ---------------------------------------------------------------------------


class TestValidationError:
    """Provoke 422 responses via invalid data types."""

    def test_invalid_email(self, rbac, base_url):
        resp = requests.post(
            f"{base_url}/rbac/v1/users",
            headers={"Authorization": f"Bearer {rbac.jwt}"},
            json={
                "email": "not-an-email",
                "name": "ValidUser_42",
                "password": "ValidPass_123",
                "details": {"test": True},
            },
            timeout=5,
        )
        assert resp.status_code == 422
        body = resp.json()
        assert body["error"] == "ValidationError"

    def test_wrong_type_for_field(self, rbac, base_url):
        resp = requests.post(
            f"{base_url}/rbac/v1/users",
            headers={"Authorization": f"Bearer {rbac.jwt}"},
            json={
                "email": "test@kronicle.app",
                "orcid": 12345,
                "password": "ValidPass_123",
                "details": {"test": True},
            },
            timeout=5,
        )
        assert resp.status_code == 422
        body = resp.json()
        assert body["error"] == "ValidationError"


# ---------------------------------------------------------------------------
# Anonymous user must not inherit permissions from other group policies
# ---------------------------------------------------------------------------


class TestAnonymousPermissionIsolation:
    """Verify that anonymous users only get permissions explicitly granted
    to the anonymous group, not from arbitrary group policies.

    Regression test: ``RbacSubject.user_id == None`` previously matched all
    group subjects via SQL ``IS NULL``, leaking permissions.
    """

    def test_anonymous_cannot_list_users(self, base_url):
        """GET /rbac/v1/users requires USER_READ — anonymous should get 401 or 403."""
        resp = requests.get(f"{base_url}/rbac/v1/users", timeout=5)
        assert resp.status_code in [401, 403]

    def test_anonymous_cannot_read_roles(self, base_url):
        """GET /rbac/v1/roles requires ROLE_READ — anonymous should get 401 or 403."""
        resp = requests.get(f"{base_url}/rbac/v1/roles", timeout=5)
        assert resp.status_code in [401, 403]

    def test_anonymous_cannot_list_groups(self, base_url):
        """GET /rbac/v1/groups requires GROUP_READ — anonymous should get 401 or 403."""
        resp = requests.get(f"{base_url}/rbac/v1/groups", timeout=5)
        assert resp.status_code in [401, 403]

    def test_anonymous_cannot_read_zones(self, base_url):
        """GET /core/v1/zones requires ZONE_READ — anonymous should get 401 or 403."""
        resp = requests.get(f"{base_url}/core/v1/zones", timeout=5)
        assert resp.status_code in [401, 403]

    def test_anonymous_cannot_read_channels(self, base_url):
        """GET /setup/v1/channels requires CHANNEL_READ — anonymous should get 401 or 403."""
        resp = requests.get(f"{base_url}/setup/v1/channels", timeout=5)
        assert resp.status_code in [401, 403]
