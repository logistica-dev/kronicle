# tests/integration/auth/test_auth_routes.py

import pytest
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.auth.kronicle_auth import KronicleUsrLogin
from kronicle_sdk.utils.log import log_d


@pytest.fixture(scope="session")
def auth_client():
    co = Settings().connection_su
    assert co
    return KronicleUsrLogin.from_connection_info(co)
    # return KronicleUsrLogin.from_connection_info(co)


def test_login(auth_client):
    log_d("auth", "jwt", auth_client.jwt)
    assert auth_client.jwt is not None
