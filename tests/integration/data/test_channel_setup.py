# tests/integration/data/test_channel_setup.py

from uuid import UUID

import pytest
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.abc_connector import KroniclePayload
from kronicle_sdk.connectors.channel.channel_setup import KronicleSetup
from kronicle_sdk.models.iso_datetime import now_local
from kronicle_sdk.utils.log import log_d, log_w
from kronicle_sdk.utils.str_utils import tiny_id, uuid4_str


@pytest.fixture(scope="session")
def kronicle_setup():
    """Return a connected KronicleSetup instance."""
    co = Settings().connection
    setup = KronicleSetup(co.url, co.usr, co.pwd)
    return setup


@pytest.mark.integration
def test_list_channels(kronicle_setup):
    """Check that all channels can be listed and max-row channel is accessible."""
    here = "ksetup"
    log_d(here, "Channel list vvv")
    for channel in kronicle_setup.all_channels:
        assert isinstance(channel, KroniclePayload)
    log_d(here, "Channel list ^^^")

    max_chan = kronicle_setup.get_channel_with_max_rows()
    if max_chan and (max_chan_id := max_chan.channel_id):
        channel = kronicle_setup.get_channel(max_chan_id)
        assert channel is not None
        rows = kronicle_setup.get_rows_for_channel(max_chan_id)
        assert isinstance(rows, list)
        for row in rows:
            assert isinstance(row, dict)
        cols = kronicle_setup.get_cols_for_channel(max_chan_id)
        assert isinstance(cols, dict)
        for col, vals in cols.items():
            assert isinstance(col, str)
            assert isinstance(vals, list)


@pytest.mark.integration
def test_insert_rows_and_upsert_channel(kronicle_setup):
    """Insert a new channel and verify it is added correctly."""
    here = "ksetup"
    channel_id = uuid4_str()
    channel_name = f"demo_channel_{tiny_id()}"
    now_tag = now_local()

    payload = {
        "channel_id": channel_id,
        "channel_name": channel_name,
        "channel_schema": {"time": "datetime", "temperature": "float"},
        "metadata": {"unit": "°C"},
        "tags": {"test": now_tag},
        "rows": [
            {"time": "2025-01-10T00:00:00Z", "temperature": 12.3},
            {"time": "2025-01-10T00:01:00Z", "temperature": 12.8},
        ],
    }
    log_d(here, "payload", payload)

    result = kronicle_setup.insert_rows_and_upsert_channel(payload)
    log_d(here, "result", result)
    log_d(here, "column types", kronicle_setup.column_types)

    assert result is not None
    assert isinstance(result, KroniclePayload)
    assert result.channel_id == UUID(channel_id)
    kronicle_setup.delete_channel(channel_id)


@pytest.mark.integration
def test_get_invalid_route_raises(kronicle_setup):
    """Verify that accessing a non-existent route raises an exception."""
    here = "ksetup"
    with pytest.raises(Exception) as exc:
        kronicle_setup.get(route="route/that/does/not/exist", strict=False)
    log_w(here, "OK, exception caught:", exc.value)
