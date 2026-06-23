# tests/integration/data/test_channel_writer.py

from uuid import UUID

import pytest
from kronicle_sdk.connectors.abc_connector import KroniclePayload
from kronicle_sdk.models.iso_datetime import IsoDateTime, now_local
from kronicle_sdk.models.rbac.kronicle_zone import KronicleZone
from kronicle_sdk.utils.log import log_d
from kronicle_sdk.utils.str_utils import tiny_id, uuid4_str


@pytest.mark.integration
def test_writer_properties(kronicle_writer):
    assert kronicle_writer.prefix == "/data/v1"


@pytest.mark.integration
def test_writer_channels(kronicle_writer, test_channel_id):
    """Check that the writer returns channels and that our test channel is accessible."""
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
def test_create_and_update_channel(kronicle_setup, kronicle_writer, kronicle_rbac_setup):
    """Create a channel in a zone, then update and insert rows via writer."""
    here = "KWrite.create"
    channel_id: str = uuid4_str()
    channel_name: str = f"demo_channel_{tiny_id()}"
    now_tag = now_local()
    tag = tiny_id()

    zone = kronicle_rbac_setup.create_zone(KronicleZone(name=f"writer_test_zone_{tag}", details={"test": True}))

    try:
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

        # Create channel in zone via writer (zone-aware data path)
        result = kronicle_writer.create_channel(zone_id=zone.id, body=payload)
        assert result is not None
        assert result.channel_id == UUID(channel_id)

        # Update channel metadata + insert more rows via writer
        payload["rows"] = [
            {"time": now_local(), "temperature": 42.0},
        ]
        result = kronicle_writer.insert_rows_and_update_channel(payload)
        log_d(here, "result", result)

        assert result is not None
        assert isinstance(result, KroniclePayload)
        assert result.channel_id == UUID(channel_id)

        # Insert additional rows directly
        result = kronicle_writer.insert_rows(
            id=channel_id,
            rows=[
                {"time": now_local(), "temperature": 12.4},
                {"time": now_local(), "temperature": 12.5},
            ],
        )
        assert result is not None
        assert isinstance(result, KroniclePayload)
        assert result.channel_id == UUID(channel_id)

        kronicle_setup.delete_channel(channel_id)
    finally:
        kronicle_rbac_setup.delete_zone(zone_id=zone.id)
