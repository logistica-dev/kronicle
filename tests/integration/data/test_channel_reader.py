# tests/integration/data/test_channel_reader.py

import pytest
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.channel.channel_reader import KronicleReader


@pytest.fixture(scope="session")
def kronicle_reader():
    """Return a connected KronicleReader."""
    co = Settings().connection_su
    assert co
    reader = KronicleReader.from_connection_info(co)
    return reader


@pytest.mark.integration
def test_reader_alive_and_ready(kronicle_reader):
    """Check that the reader reports alive and ready."""
    assert kronicle_reader.is_alive() is True
    assert kronicle_reader.is_ready() is True


@pytest.mark.integration
def test_reader_channels(kronicle_reader, test_channel_id):
    """Check that the reader returns channels and max-row channel."""
    all_channels = kronicle_reader.all_channels

    assert isinstance(all_channels, list)
    assert len(all_channels) > 0, "Expected at least one channel"

    channel = kronicle_reader.get_channel(test_channel_id)
    assert channel is not None
    rows = kronicle_reader.get_rows_for_channel(test_channel_id)
    assert isinstance(rows, list)
    for row in rows:
        assert isinstance(row, dict)
    cols = kronicle_reader.get_cols_for_channel(test_channel_id)
    assert isinstance(cols, dict)
    for col, vals in cols.items():
        assert isinstance(col, str)
        assert isinstance(vals, list)
