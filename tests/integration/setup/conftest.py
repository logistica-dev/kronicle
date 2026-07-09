# tests/integration/setup/conftest.py
from collections.abc import Generator

import pytest
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.channel.channel_setup import KronicleSetup
from kronicle_sdk.connectors.channel.channel_writer import KronicleWriter
from kronicle_sdk.connectors.rbac.core_setup import KronicleCore
from kronicle_sdk.models.rbac.kronicle_zone import KronicleZone
from kronicle_sdk.utils.str_utils import tiny_id, uuid4_str


@pytest.fixture(scope="session")
def kronicle_rbac_setup():
    co = Settings().connection_su
    assert co
    return KronicleCore.from_connection_info(co)


@pytest.fixture(scope="session")
def kronicle_setup():
    co = Settings().connection_su
    assert co
    return KronicleSetup.from_connection_info(co)


@pytest.fixture(scope="session")
def kronicle_writer():
    co = Settings().connection_su
    assert co
    return KronicleWriter.from_connection_info(co)


@pytest.fixture(scope="module")
def test_zone(kronicle_rbac_setup) -> Generator[str, None, None]:
    tag = tiny_id()
    zone = kronicle_rbac_setup.create_zone(KronicleZone(name=f"setup_zone_{tag}", details={"test": True}))
    yield str(zone.id)
    try:
        kronicle_rbac_setup.delete_zone(zone_id=zone.id)
    except Exception:
        pass


@pytest.fixture(scope="module")
def test_channel(kronicle_writer, kronicle_setup, test_zone) -> Generator[str, None, None]:
    channel_id = uuid4_str()
    name = f"sync_chan_{tiny_id()}"
    payload = {
        "id": channel_id,
        "name": name,
        "channel_schema": {"time": "datetime", "value": "float"},
        "metadata": {"source": "sync-test"},
        "tags": {"test": "true"},
        "rows": [
            {"time": "2025-01-10T00:00:00Z", "value": 1.0},
        ],
    }
    kronicle_writer.create_channel(zone_id=test_zone, body=payload)
    yield channel_id
    try:
        kronicle_setup.delete_channel(channel_id)
    except Exception:
        pass
