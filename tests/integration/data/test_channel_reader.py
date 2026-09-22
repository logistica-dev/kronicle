# tests/integration/data/test_channel_reader.py

import pytest
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.abc_connector import KronicleChannel
from kronicle_sdk.connectors.channel.channel_reader import KronicleReader
from kronicle_sdk.utils.str_utils import tiny_id, uuid4_str


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

    channel = kronicle_reader.get_channel_by_id(test_channel_id)
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


@pytest.mark.integration
def test_get_channel_by_name_normalization(kronicle_reader, kronicle_writer, kronicle_setup, test_zone):
    """Fetch-by-name must use the same normalization as the write path."""
    names = [f"a__b_{tiny_id()}", f"x-y-{tiny_id()}", f"trail_{tiny_id()}_"]
    created_ids = []
    try:
        for name in names:
            payload = KronicleChannel.from_json(
                {
                    "id": uuid4_str(),
                    "name": name,
                    "channel_schema": {"time": "datetime", "value": "float"},
                    "metadata": {"source": "integration-test"},
                }
            )
            stored = kronicle_writer.create_channel(zone_id=test_zone, body=payload)
            created_ids.append(stored.id)
            assert stored.name  # server-returned, strictly normalized

            by_raw = kronicle_reader.get_channel_by_name(name)
            assert by_raw is not None, f"raw name '{name}' must match stored '{stored.name}'"
            assert by_raw.id == stored.id

            by_stored = kronicle_reader.get_channel_by_name(stored.name)
            assert by_stored is not None
            assert by_stored.id == stored.id
    finally:
        for cid in created_ids:
            try:
                kronicle_setup.delete_channel(cid)
            except Exception:
                pass
