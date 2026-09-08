# tests/integration/core/test_core_api.py
"""Exercises the core zone/channel routes (PATCH zone, PATCH/DELETE core channel, list zone channels)."""

import pytest
from kronicle_sdk.models.rbac.kronicle_zone import KronicleZone
from kronicle_sdk.utils.log import log_d
from kronicle_sdk.utils.str_utils import tiny_id


@pytest.mark.integration
def test_api_patch_zone(kronicle_core):
    """PATCH /core/v1/zones/{zone_id}."""
    here = "core_zones"
    tag = tiny_id()
    zone = kronicle_core.create_zone(KronicleZone(name=f"patch_zone_{tag}", details={"test": True}))
    try:
        patch = KronicleZone(id=zone.id, name=f"patched_zone_{tag}")
        patched = kronicle_core.patch_zone(patch)
        log_d(here, "patched", patched)
        assert patched is not None
        assert patched.name == f"patched_zone_{tag}"
    finally:
        kronicle_core.delete_zone(zone_id=zone.id)


@pytest.mark.integration
def test_api_list_zone_channels(kronicle_core, test_zone, test_channel):
    """GET /core/v1/zones/{zone_id}/channels."""
    channels = kronicle_core.list_core_channels(zone_id=test_zone.id)
    assert isinstance(channels, list)
    assert any(str(c.id) == str(test_channel.id) for c in channels)


@pytest.mark.integration
def test_api_patch_core_channel(kronicle_core, test_channel):
    """PATCH /core/v1/channels/{channel_id}."""
    original = test_channel.name
    new_name = f"{original}_patched"
    patched = kronicle_core.patch_core_channel(channel_id=test_channel.id, name=new_name)
    assert patched is not None
    assert patched.name == new_name
    # Restore the original name so sibling tests are not affected
    restored = kronicle_core.patch_core_channel(channel_id=test_channel.id, name=original)
    assert restored is not None
    assert restored.name == original


@pytest.mark.integration
def test_api_delete_core_channel(kronicle_core, kronicle_setup, test_zone):
    """DELETE /core/v1/channels/{channel_id} removes the core record."""
    tag = tiny_id()
    channel = kronicle_setup.create_channel(
        {"name": f"core_del_{tag}", "channel_schema": {"time": "datetime", "val": "float"}},
        zone_id=test_zone.id,
    )
    channel_id = channel.id
    try:
        deleted = kronicle_core.delete_core_channel(channel_id)
        assert deleted is not None
        remaining = kronicle_core.list_core_channels()
        assert all(str(c.id) != str(channel_id) for c in remaining)
    finally:
        try:
            kronicle_setup.delete_channel(channel_id)
        except Exception:
            pass
