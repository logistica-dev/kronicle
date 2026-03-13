# tests/integration/auth/auth_routes.py

import pytest
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.auth.kronicle_auth import KronicleUsrLogin
from kronicle_sdk.utils.log import log_d


@pytest.fixture(scope="session")
def auth_client():
    co = Settings().connection
    return KronicleUsrLogin(co.url, co.usr, co.pwd)


def test_login(auth_client):
    log_d("auth", "jwt", auth_client.jwt)
    assert auth_client.jwt is not None
