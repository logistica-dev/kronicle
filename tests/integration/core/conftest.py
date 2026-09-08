# tests/integration/core/conftest.py
from collections.abc import Generator

import pytest
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.channel.channel_setup import KronicleSetup
from kronicle_sdk.connectors.rbac.core_setup import KronicleCore
from kronicle_sdk.models.data.kronicle_channel import KronicleChannel
from kronicle_sdk.models.rbac.kronicle_zone import KronicleZone
from kronicle_sdk.utils.str_utils import tiny_id


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
def test_zone(kronicle_core) -> Generator[KronicleZone, None, None]:
    tag = tiny_id()
    zone = kronicle_core.create_zone(KronicleZone(name=f"core_zone_{tag}", details={"test": True}))
    yield zone
    try:
        kronicle_core.delete_zone(zone_id=zone.id)
    except Exception:
        pass


@pytest.fixture(scope="module")
def test_channel(kronicle_setup, test_zone) -> Generator[KronicleChannel, None, None]:
    tag = tiny_id()
    channel = KronicleChannel(
        name=f"core_channel_{tag}",
        channel_schema={"time": "datetime", "value": "float"},
        rows=[{"time": "2025-01-10T00:00:00Z", "value": 1.0}],
        metadata={"source": "core-test"},
        tags={"test": True},
    )
    created = kronicle_setup.create_channel(channel, zone_id=test_zone.id)
    yield created
    try:
        kronicle_setup.delete_channel(created.id)
    except Exception:
        pass
