# tests/integration/rbac/test_rbac_api_bulk.py
"""Exercises the bulk PATCH / DELETE /rbac/v1/users endpoints (no id in the path).

These routes take a full user body and resolve the target user by email.
"""

from collections.abc import Generator

import pytest
import requests as req
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.rbac.rbac_setup import KronicleRbac
from kronicle_sdk.models.rbac.kronicle_user import KronicleUser
from kronicle_sdk.utils.str_utils import tiny_id

pytestmark = pytest.mark.integration


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


@pytest.fixture(scope="module")
def bulk_user(su_client) -> Generator[KronicleUser, None, None]:
    tag = tiny_id()
    user = KronicleUser(
        email=f"bulk_{tag}@kronicle.app",
        name=f"bulk_user_{tag}",
        password="BulkPass_123!",
        details={"test": True},
    )
    created = su_client.create_user(user)
    yield created
    try:
        su_client.delete_user(user_id=created.id)
    except Exception:
        pass


def _patch_bulk(base_url, jwt: str, user: KronicleUser) -> req.Response:
    return req.patch(
        f"{base_url}/rbac/v1/users",
        headers={"Authorization": f"Bearer {jwt}"},
        json={
            "id": str(user.id),
            "email": user.email,
            "name": user.name,
            "details": {"test": True},
        },
        timeout=10,
    )


def _delete_bulk(base_url, jwt: str, user: KronicleUser) -> req.Response:
    return req.delete(
        f"{base_url}/rbac/v1/users",
        headers={"Authorization": f"Bearer {jwt}"},
        json={"id": str(user.id), "email": user.email, "name": user.name},
        timeout=10,
    )


def test_api_patch_users_bulk(su_client, base_url, bulk_user):
    """PATCH /rbac/v1/users updates a user resolved from the body email."""
    patched_name = f"{bulk_user.name}_patched"
    patch = KronicleUser(id=bulk_user.id, email=bulk_user.email, name=patched_name)
    resp = _patch_bulk(base_url, su_client.jwt, patch)
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == patched_name

    # Verify the change persisted
    fetched = su_client.get_user_by_id(user_id=bulk_user.id)
    assert fetched is not None
    assert fetched.name == patched_name


def test_api_delete_users_bulk_deactivates(su_client, base_url, bulk_user):
    """DELETE /rbac/v1/users (no ?remove=true) deactivates the user."""
    resp = _delete_bulk(base_url, su_client.jwt, bulk_user)
    assert resp.status_code == 200
    body = resp.json()
    assert body["email"] == bulk_user.email

    # The deleted user should not appear in the default (active-only) listing
    users = su_client.list_users()
    assert str(bulk_user.id) not in [str(u.id) for u in users]
