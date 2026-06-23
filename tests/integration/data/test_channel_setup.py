# tests/integration/data/test_channel_setup.py

from uuid import UUID

import pytest
from kronicle_sdk.connectors.abc_connector import KroniclePayload
from kronicle_sdk.models.iso_datetime import now_local
from kronicle_sdk.models.rbac.kronicle_zone import KronicleZone
from kronicle_sdk.utils.log import log_d, log_w
from kronicle_sdk.utils.str_utils import tiny_id, uuid4_str


@pytest.mark.integration
def test_list_channels(kronicle_setup, test_channel_id):
    """Check that all channels can be listed and that our test channel is accessible."""
    here = "ksetup"
    log_d(here, "Channel list vvv")
    for channel in kronicle_setup.all_channels:
        assert isinstance(channel, KroniclePayload)
    log_d(here, "Channel list ^^^")

    channel = kronicle_setup.get_channel(test_channel_id)
    assert channel is not None
    rows = kronicle_setup.get_rows_for_channel(test_channel_id)
    assert isinstance(rows, list)
    for row in rows:
        assert isinstance(row, dict)
    cols = kronicle_setup.get_cols_for_channel(test_channel_id)
    assert isinstance(cols, dict)
    for col, vals in cols.items():
        assert isinstance(col, str)
        assert isinstance(vals, list)


@pytest.mark.integration
def test_insert_rows_and_upsert_channel(kronicle_writer, kronicle_setup, kronicle_rbac_setup):
    """Insert a new channel in a zone and verify it is added correctly."""
    here = "ksetup"
    channel_id = uuid4_str()
    channel_name = f"demo_channel_{tiny_id()}"
    now_tag = now_local()
    tag = tiny_id()

    zone = kronicle_rbac_setup.create_zone(KronicleZone(name=f"setup_test_zone_{tag}", details={"test": True}))

    try:
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

        result = kronicle_writer.create_channel(zone_id=zone.id, body=payload)
        log_d(here, "result", result)
        log_d(here, "column types", kronicle_setup.column_types)

        assert result is not None
        assert isinstance(result, KroniclePayload)
        assert result.channel_id == UUID(channel_id)

        kronicle_setup.delete_channel(channel_id)
    finally:
        kronicle_rbac_setup.delete_zone(zone_id=zone.id)


@pytest.mark.integration
def test_get_invalid_route_raises(kronicle_setup):
    """Verify that accessing a non-existent route raises an exception."""
    here = "ksetup"
    with pytest.raises(Exception) as exc:
        kronicle_setup.get(route="route/that/does/not/exist", strict=False)
    log_w(here, "OK, exception caught:", exc.value)
