# tests/integration/rbac/conftest.py
from collections.abc import Generator

import pytest
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.rbac.rbac_setup import KronicleRbac
from kronicle_sdk.models.rbac.kronicle_group import KronicleGroup
from kronicle_sdk.models.rbac.kronicle_user import KronicleUser
from kronicle_sdk.utils.str_utils import tiny_id


@pytest.fixture(scope="session")
def kronicle_rbac():
    co = Settings().connection_su
    assert co
    return KronicleRbac.from_connection_info(co)


@pytest.fixture(scope="module")
def test_user(kronicle_rbac) -> Generator[KronicleUser, None, None]:
    tag = tiny_id()
    user = KronicleUser(
        email=f"test_{tag}@kronicle.app",
        name=f"test_user_{tag}",
        password="TestPass_123",
        details={"test": True},
    )
    created = kronicle_rbac.create_user(user)
    yield created
    try:
        kronicle_rbac.delete_user(user_id=created.id)
    except Exception:
        pass


@pytest.fixture(scope="module")
def test_group(kronicle_rbac) -> Generator[KronicleGroup, None, None]:
    tag = tiny_id()
    group = KronicleGroup(name=f"test_group_{tag}", details={"test": True})
    created = kronicle_rbac.create_group(group)
    yield created
    try:
        kronicle_rbac.delete_group(group_id=created.id)
    except Exception:
        pass
