from collections.abc import Generator

import pytest
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.rbac.rbac_identity_setup import KronicleRbacIdentitySetup
from kronicle_sdk.models.rbac.kronicle_user import KronicleUser
from kronicle_sdk.utils.str_utils import tiny_id


@pytest.fixture(scope="session")
def kronicle_rbac():
    co = Settings().connection_su
    assert co
    return KronicleRbacIdentitySetup(co.url, co.usr, co.pwd)


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
    kronicle_rbac.remove_user_by_id(created.id)
