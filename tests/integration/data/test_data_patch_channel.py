# tests/integration/data/test_data_patch_channel.py
"""Exercises PATCH /data/v1/channels/{channel_id} via the writer connector."""

import pytest
from kronicle_sdk.models.data.kronicle_channel import KronicleChannel
from kronicle_sdk.utils.log import log_d


@pytest.mark.integration
def test_api_patch_channel_metadata(kronicle_writer, test_channel_id):
    """Patch channel metadata/tags through the data writer."""
    here = "patch_channel"
    patch = KronicleChannel(
        id=test_channel_id,
        metadata={"patched_by": "data-patch-test"},
        tags={"test": "true", "source": "patch-test"},
    )
    result = kronicle_writer.patch_channel(patch)
    log_d(here, "patch result", result)
    assert result is not None

    channel = kronicle_writer.get_channel(test_channel_id)
    assert channel is not None
    assert channel.metadata.get("patched_by") == "data-patch-test"
    assert channel.tags.get("source") == "patch-test"
