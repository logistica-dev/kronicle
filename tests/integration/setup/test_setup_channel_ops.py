# tests/integration/setup/test_setup_channel_ops.py
"""Exercises the setup channel routes: update_channel, insert_rows, delete all rows, batch-delete."""

import pytest
import requests as req
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.models.data.kronicle_channel import KronicleChannel
from kronicle_sdk.utils.log import log_d
from kronicle_sdk.utils.str_utils import tiny_id


@pytest.fixture(scope="session")
def base_url() -> str:
    co = Settings().connection_su
    assert co
    return co.url


@pytest.mark.integration
def test_api_update_channel(kronicle_setup, test_channel):
    """PATCH /setup/v1/channels/{channel_id}."""
    here = "setup_ops"
    body = {
        "id": test_channel,
        "metadata": {"patched_by": "setup-update-test"},
        "tags": {"test": "true"},
    }
    result = kronicle_setup.update_channel(body)
    log_d(here, "update result", result)
    assert result is not None

    channel: KronicleChannel | None = kronicle_setup.get_channel(test_channel)
    assert channel is not None
    assert channel.metadata is not None
    assert channel.metadata.get("patched_by") == "setup-update-test"


@pytest.mark.integration
def test_api_insert_rows_setup(kronicle_setup, test_channel):
    """POST /setup/v1/channels/{channel_id}/rows."""
    before = kronicle_setup.get_rows_for_channel(test_channel, "dict")
    assert isinstance(before, list)

    result = kronicle_setup.insert_rows(
        id=test_channel,
        rows=[{"time": "2025-03-01T00:00:00Z", "value": 7.7}],
    )
    assert result is not None

    after = kronicle_setup.get_rows_for_channel(test_channel, "dict")
    assert isinstance(after, list)
    assert len(after) == len(before) + 1


@pytest.mark.integration
def test_api_delete_all_rows_setup(kronicle_setup, test_channel):
    """DELETE /setup/v1/channels/{channel_id}/rows removes data but keeps metadata."""
    result = kronicle_setup.delete(route=f"channels/{test_channel}/rows")
    assert result is not None

    rows = kronicle_setup.get_rows_for_channel(test_channel, "dict")
    assert rows in (None, [])

    # Metadata survives the row deletion
    channel = kronicle_setup.get_channel(test_channel)
    assert channel is not None


@pytest.mark.integration
def test_api_batch_delete_channels(kronicle_writer, kronicle_setup, test_zone, base_url):
    """POST /setup/v1/channels/batch-delete removes several channels at once."""
    ids = []
    for _ in range(2):
        channel = kronicle_writer.create_channel(
            {"name": f"batch_del_{tiny_id()}", "channel_schema": {"time": "datetime", "val": "float"}},
            zone_id=test_zone,
        )
        ids.append(str(channel.id))
    log_d("setup_ops", "created channels", ids)

    # A body missing channel_ids must validate (422), not 500.
    bad = req.post(
        f"{base_url}/setup/v1/channels/batch-delete",
        headers={"Authorization": f"Bearer {kronicle_setup.jwt}"},
        json={},
        timeout=10,
    )
    assert bad.status_code == 422

    resp = req.post(
        f"{base_url}/setup/v1/channels/batch-delete",
        headers={"Authorization": f"Bearer {kronicle_setup.jwt}"},
        json={"channel_ids": ids},
        timeout=10,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body, list)
    assert len(body) == len(ids)

    remaining = [str(c.id) for c in kronicle_setup.list_channels()]
    assert all(cid not in remaining for cid in ids)
