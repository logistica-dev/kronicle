from collections.abc import Generator

import pytest
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.channel.channel_setup import KronicleSetup
from kronicle_sdk.utils.str_utils import tiny_id, uuid4_str


@pytest.fixture(scope="session")
def kronicle_setup():
    co = Settings().connection_su
    assert co
    return KronicleSetup(co.url, co.usr, co.pwd)


@pytest.fixture(scope="module")
def test_channel_id(kronicle_setup) -> Generator[str, None, None]:
    channel_id = uuid4_str()
    channel_name = f"test_chan_{tiny_id()}"
    payload = {
        "channel_id": channel_id,
        "channel_name": channel_name,
        "channel_schema": {"time": "datetime", "value": "float"},
        "metadata": {"source": "integration-test"},
        "tags": {"test": "true"},
        "rows": [
            {"time": "2025-01-10T00:00:00Z", "value": 1.0},
            {"time": "2025-01-10T00:01:00Z", "value": 2.0},
        ],
    }
    kronicle_setup.insert_rows_and_upsert_channel(payload)
    yield channel_id
    kronicle_setup.delete_channel(channel_id)
