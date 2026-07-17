# tests/integration/rbac/conftest.py
from collections.abc import Generator

import pytest
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.channel.channel_setup import KronicleSetup
from kronicle_sdk.connectors.rbac.core_setup import KronicleCore
from kronicle_sdk.connectors.rbac.rbac_setup import KronicleRbac
from kronicle_sdk.models.data.kronicle_channel import KronicleChannel
from kronicle_sdk.models.rbac.kronicle_group import KronicleGroup
from kronicle_sdk.models.rbac.kronicle_user import KronicleUser
from kronicle_sdk.models.rbac.kronicle_zone import KronicleZone
from kronicle_sdk.utils.str_utils import tiny_id


@pytest.fixture(scope="session")
def kronicle_rbac():
    co = Settings().connection_su
    assert co
    return KronicleRbac.from_connection_info(co)


@pytest.fixture(scope="session")
def kronicle_core():
    co = Settings().connection_su
    assert co
    return KronicleCore.from_connection_info(co)


@pytest.fixture(scope="session")
def kronicle_setup():
    co = Settings().connection_su
    assert co
    return KronicleSetup.from_connection_info(co)


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
        kronicle_rbac.delete_group(group_id=created.id, force=True)
    except Exception:
        pass


@pytest.fixture(scope="module")
def test_zone(kronicle_core) -> Generator[KronicleZone, None, None]:
    tag = tiny_id()
    zone = KronicleZone(name=f"test_zone_{tag}", details={"test": True})
    created = kronicle_core.create_zone(zone)
    yield created
    try:
        kronicle_core.delete_zone(zone_id=created.id)
    except Exception:
        pass


@pytest.fixture(scope="module")
def test_channel(kronicle_setup, test_zone) -> Generator[KronicleChannel, None, None]:
    tag = tiny_id()
    channel = KronicleChannel(
        name=f"test_channel_{tag}",
        channel_schema={"time": "datetime", "value": "float"},
        rows=[
            {"time": "2025-01-10T00:00:00Z", "value": 1.0},
            {"time": "2025-01-11T00:00:00Z", "value": 2.0},
        ],
        metadata={"source": "integration-test"},
        tags={"test": True},
    )
    created = kronicle_setup.create_channel(channel, zone_id=test_zone.id)
    yield created
    try:
        kronicle_setup.delete_channel(created.id)
    except Exception:
        pass


@pytest.fixture(scope="module")
def test_row_id(test_channel):
    """Return the ID of the first row in the test channel."""
    rows = test_channel.rows or []
    assert rows, "Test channel has no rows"
    return rows[0]["id"]
