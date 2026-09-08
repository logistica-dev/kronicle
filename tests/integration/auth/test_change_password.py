# tests/integration/auth/test_change_password.py

from collections.abc import Generator

import pytest
import requests as req
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.rbac.rbac_setup import KronicleRbac
from kronicle_sdk.models.rbac.kronicle_user import KronicleUser
from kronicle_sdk.utils.str_utils import slash_join, tiny_id

pytestmark = pytest.mark.integration


def _jwt(url: str, login: str, password: str) -> str:
    r = req.post(slash_join(url, "auth/v1/login"), json={"login": login, "password": password}, timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]


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
def pwd_user(su_client) -> Generator[dict, None, None]:
    """A dedicated user whose password we are allowed to rotate."""
    tag = tiny_id()
    email = f"pwd_{tag}@kronicle.app"
    password = "OldPass_123!"
    user = su_client.create_user(
        KronicleUser(email=email, name=f"pwd_user_{tag}", password=password, details={"test": True})
    )
    yield {"id": user.id, "email": email, "password": password}
    try:
        su_client.delete_user(user_id=user.id)
    except Exception:
        pass


def test_change_password_rotates_and_reissues_token(base_url, su_client, pwd_user):
    """POST /auth/v1/change_password returns a fresh token and invalidates the old password."""
    old = pwd_user["password"]
    new = "NewPass_456!"

    # Old credentials work before the rotation
    _jwt(base_url, pwd_user["email"], old)

    resp = req.post(
        slash_join(base_url, "auth/v1/change_password"),
        json={
            "login": pwd_user["email"],
            "password": old,
            "new_password": new,
            "confirm_password": new,
        },
        timeout=10,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["token_type"] == "bearer"
    assert isinstance(body["access_token"], str) and body["access_token"]

    # Old password is rejected, new one works
    old_resp = req.post(
        slash_join(base_url, "auth/v1/login"),
        json={"login": pwd_user["email"], "password": old},
        timeout=10,
    )
    assert old_resp.status_code == 401
    _jwt(base_url, pwd_user["email"], new)


def test_change_password_rejects_mismatched_confirmation(base_url, pwd_user):
    """change_password fails (400/422) when new_password != confirm_password."""
    resp = req.post(
        slash_join(base_url, "auth/v1/change_password"),
        json={
            "login": pwd_user["email"],
            "password": pwd_user["password"],
            "new_password": "NewPass_456!",
            "confirm_password": "Different_789!",
        },
        timeout=10,
    )
    assert resp.status_code in [400, 422]
