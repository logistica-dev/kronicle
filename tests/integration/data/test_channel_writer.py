# tests/integration/data/test_channel_writer.py

from uuid import UUID

import pytest
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.abc_connector import KroniclePayload
from kronicle_sdk.connectors.channel.channel_writer import KronicleWriter
from kronicle_sdk.models.iso_datetime import IsoDateTime, now_local
from kronicle_sdk.utils.log import log_d
from kronicle_sdk.utils.str_utils import tiny_id, uuid4_str


@pytest.fixture(scope="session")
def kronicle_writer():
    """Return a connected KronicleWriter."""
    co = Settings().connection
    writer = KronicleWriter(co.url, co.usr, co.pwd)
    return writer


@pytest.mark.integration
def test_writer_properties(kronicle_writer):
    assert kronicle_writer.prefix == "/data/v1"


@pytest.mark.integration
def test_writer_channels(kronicle_writer, test_channel_id):
    """Check that the writer returns channels and that our test channel is accessible."""
    here = "KWrite.chans"
    all_channels = kronicle_writer.all_channels
    for channel in all_channels:
        assert isinstance(channel, KroniclePayload)

    channel = kronicle_writer.get_channel(test_channel_id)
    assert channel is not None
    rows = kronicle_writer.get_rows_for_channel(test_channel_id, "list")
    assert isinstance(rows, list)
    for row in rows:
        assert isinstance(row, dict)
    assert len(rows)


@pytest.mark.integration
def test_insert_rows_and_upsert_channel(kronicle_writer, kronicle_setup):
    """Insert a new channel with sample rows and verify result."""
    here = "KWrite.insert"
    channel_id: str = uuid4_str()
    channel_name: str = f"demo_channel_{tiny_id()}"
    now_tag = now_local()

    payload = {
        "channel_id": channel_id,
        "channel_name": channel_name,
        "channel_schema": {"time": IsoDateTime, "temperature": float},
        "metadata": {"unit": "°C"},
        "tags": {"test": now_tag},
        "rows": [
            {"time": now_local(), "temperature": 12.3},
            {"time": now_local(), "temperature": 12.8},
        ],
    }
    log_d(here, "payload", payload)

    result = kronicle_writer.insert_rows_and_upsert_channel(payload)
    result = kronicle_writer.add_row(payload)
    result = kronicle_writer.insert_rows(
        id=channel_id,
        rows=[
            {"time": now_local(), "temperature": 12.4},
            {"time": now_local(), "temperature": 12.5},
        ],
    )
    log_d(here, "result", result)

    # Basic assertions
    assert result is not None
    assert isinstance(result, KroniclePayload)
    assert result.channel_id == UUID(channel_id)

    kronicle_setup.delete_channel(channel_id)
