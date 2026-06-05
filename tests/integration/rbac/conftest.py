from collections.abc import Generator

import pytest
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.rbac.rbac_connector import KronicleRbacConnector
from kronicle_sdk.models.rbac.kronicle_user import KronicleUser
from kronicle_sdk.utils.str_utils import tiny_id


@pytest.fixture(scope="session")
def kronicle_rbac():
    co = Settings().connection
    return KronicleRbacConnector(co.url, co.usr, co.pwd)


@pytest.fixture(scope="module")
def test_user(kronicle_rbac) -> Generator[KronicleUser, None, None]:
    tag = tiny_id()
    user = KronicleUser(
        email=f"test_{tag}@kronicle.app",
        name=f"test_user_{tag}",
        password="TestPass_123",
    )
    created = kronicle_rbac.create_user(user)
    yield created
    try:
        kronicle_rbac.deactivate_user(created)
    except Exception:
        pass
