# tests/integration/rbac/test_rbac_api_access_profiles.py
"""Integration tests for RBAC access profile CRUD endpoints.

Tests zone, channel, and row access profile lifecycle:
create → get → list → patch → delete

Requires a running server with valid KRONICLE_SU_NAME / KRONICLE_SU_PASS env vars.
"""

from collections.abc import Generator
from uuid import UUID

import pytest
from kronicle_sdk.models.rbac.kronicle_access_profile import (
    KronicleChannelAccess,
    KronicleRowAccess,
    KronicleZoneAccess,
)
from kronicle_sdk.models.rbac.kronicle_role import KronicleRole
from kronicle_sdk.models.rbac.kronicle_row import KronicleRow
from kronicle_sdk.utils.str_utils import tiny_id

pytestmark = pytest.mark.integration


# ==============================================================================
# Fixtures
# ==============================================================================


@pytest.fixture(scope="module")
def profile_role(kronicle_rbac) -> Generator[KronicleRole, None, None]:
    tag = tiny_id()
    role = kronicle_rbac.create_role(
        KronicleRole(name=f"profile_role_{tag}", permissions=["channel:read"], details={"test": True})
    )
    yield role
    try:
        kronicle_rbac.delete_role(role_id=role.id, force=True)
    except Exception:
        pass


# ==============================================================================
# Zone access profiles
# ==============================================================================


class TestApiZoneAccessProfiles:
    def test_api_create_zone_access_profile(self, kronicle_rbac, test_zone, profile_role):
        tag = tiny_id()
        access = KronicleZoneAccess(
            name=f"zone_ap_{tag}",
            role=profile_role,
            zone=test_zone,
            description=f"Test zone access profile {tag}",
            details={"test": True},
        )
        created = kronicle_rbac.create_zone_access_profile(access)
        assert isinstance(created, KronicleZoneAccess)
        assert created.id is not None
        try:
            fetched = kronicle_rbac.get_zone_access_profile(profile_id=created.id)
            assert fetched.id == created.id
        finally:
            kronicle_rbac.delete_zone_access_profile(profile_id=created.id)

    def test_api_list_zone_access_profiles(self, kronicle_rbac, test_zone, profile_role):
        tag = tiny_id()
        access = KronicleZoneAccess(
            name=f"zone_ap_list_{tag}",
            role=profile_role,
            zone=test_zone,
        )
        created = kronicle_rbac.create_zone_access_profile(access)
        try:
            profiles = kronicle_rbac.list_zone_access_profiles()
            assert isinstance(profiles, list)
            ids = [p.id for p in profiles]
            assert created.id in ids
        finally:
            kronicle_rbac.delete_zone_access_profile(profile_id=created.id)

    def test_api_patch_zone_access_profile(self, kronicle_rbac, test_zone, profile_role):
        tag = tiny_id()
        access = KronicleZoneAccess(
            name=f"zone_ap_patch_{tag}",
            role=profile_role,
            zone=test_zone,
        )
        created = kronicle_rbac.create_zone_access_profile(access)
        try:
            created.description = f"Updated description {tag}"
            updated = kronicle_rbac.patch_zone_access_profile(access_profile=created)
            assert updated.description == f"Updated description {tag}"
        finally:
            kronicle_rbac.delete_zone_access_profile(profile_id=created.id)

    def test_api_delete_zone_access_profile(self, kronicle_rbac, test_zone, profile_role):
        tag = tiny_id()
        access = KronicleZoneAccess(
            name=f"zone_ap_del_{tag}",
            role=profile_role,
            zone=test_zone,
        )
        created = kronicle_rbac.create_zone_access_profile(access)
        deleted = kronicle_rbac.delete_zone_access_profile(profile_id=created.id)
        assert deleted.id == created.id


# ==============================================================================
# Channel access profiles
# ==============================================================================


class TestApiChannelAccessProfiles:
    def test_api_create_channel_access_profile(self, kronicle_rbac, test_channel, profile_role):
        tag = tiny_id()
        access = KronicleChannelAccess(
            name=f"chan_ap_{tag}",
            role=profile_role,
            channel=test_channel,
            description=f"Test channel access profile {tag}",
            details={"test": True},
        )
        created = kronicle_rbac.create_channel_access_profile(access)
        assert isinstance(created, KronicleChannelAccess)
        assert created.id is not None
        try:
            fetched = kronicle_rbac.get_channel_access_profile(profile_id=created.id)
            assert fetched.id == created.id
        finally:
            kronicle_rbac.delete_channel_access_profile(profile_id=created.id)

    def test_api_list_channel_access_profiles(self, kronicle_rbac, test_channel, profile_role):
        tag = tiny_id()
        access = KronicleChannelAccess(
            name=f"chan_ap_list_{tag}",
            role=profile_role,
            channel=test_channel,
        )
        created = kronicle_rbac.create_channel_access_profile(access)
        try:
            profiles = kronicle_rbac.list_channel_access_profiles()
            assert isinstance(profiles, list)
            ids = [p.id for p in profiles]
            assert created.id in ids
        finally:
            kronicle_rbac.delete_channel_access_profile(profile_id=created.id)

    def test_api_patch_channel_access_profile(self, kronicle_rbac, test_channel, profile_role):
        tag = tiny_id()
        access = KronicleChannelAccess(
            name=f"chan_ap_patch_{tag}",
            role=profile_role,
            channel=test_channel,
        )
        created = kronicle_rbac.create_channel_access_profile(access)
        try:
            created.description = f"Updated description {tag}"
            updated = kronicle_rbac.patch_channel_access_profile(access_profile=created)
            assert updated.description == f"Updated description {tag}"
        finally:
            kronicle_rbac.delete_channel_access_profile(profile_id=created.id)

    def test_api_delete_channel_access_profile(self, kronicle_rbac, test_channel, profile_role):
        tag = tiny_id()
        access = KronicleChannelAccess(
            name=f"chan_ap_del_{tag}",
            role=profile_role,
            channel=test_channel,
        )
        created = kronicle_rbac.create_channel_access_profile(access)
        deleted = kronicle_rbac.delete_channel_access_profile(profile_id=created.id)
        assert deleted.id == created.id


# ==============================================================================
# Row access profiles
# ==============================================================================


class TestApiRowAccessProfiles:
    def test_api_create_row_access_profile(self, kronicle_rbac, test_channel, test_row_id, profile_role):
        tag = tiny_id()
        access = KronicleRowAccess(
            name=f"row_ap_{tag}",
            role=profile_role,
            row=KronicleRow(id=test_row_id, channel_id=test_channel.id),
            description=f"Test row access profile {tag}",
            details={"test": True},
        )
        created = kronicle_rbac.create_row_access_profile(access)
        assert isinstance(created, KronicleRowAccess)
        assert created.id is not None
        try:
            fetched = kronicle_rbac.get_row_access_profile(profile_id=created.id)
            assert fetched.id == created.id
        finally:
            kronicle_rbac.delete_row_access_profile(profile_id=created.id)

    def test_api_list_row_access_profiles(self, kronicle_rbac, test_channel, test_row_id, profile_role):
        tag = tiny_id()
        access = KronicleRowAccess(
            name=f"row_ap_list_{tag}",
            role=profile_role,
            row=KronicleRow(id=test_row_id, channel_id=test_channel.id),
        )
        created = kronicle_rbac.create_row_access_profile(access)
        try:
            profiles = kronicle_rbac.list_row_access_profiles()
            assert isinstance(profiles, list)
            ids = [p.id for p in profiles]
            assert created.id in ids
        finally:
            kronicle_rbac.delete_row_access_profile(profile_id=created.id)

    def test_api_patch_row_access_profile(self, kronicle_rbac, test_channel, test_row_id, profile_role):
        tag = tiny_id()
        access = KronicleRowAccess(
            name=f"row_ap_patch_{tag}",
            role=profile_role,
            row=KronicleRow(id=test_row_id, channel_id=test_channel.id),
        )
        created = kronicle_rbac.create_row_access_profile(access)
        try:
            created.description = f"Updated description {tag}"
            updated = kronicle_rbac.patch_row_access_profile(access_profile=created)
            assert updated.description == f"Updated description {tag}"
        finally:
            kronicle_rbac.delete_row_access_profile(profile_id=created.id)

    def test_api_delete_row_access_profile(self, kronicle_rbac, test_channel, test_row_id, profile_role):
        tag = tiny_id()
        access = KronicleRowAccess(
            name=f"row_ap_del_{tag}",
            role=profile_role,
            row=KronicleRow(id=test_row_id, channel_id=test_channel.id),
        )
        created = kronicle_rbac.create_row_access_profile(access)
        deleted = kronicle_rbac.delete_row_access_profile(profile_id=created.id)
        assert deleted.id == created.id


# ==============================================================================
# Access profiles listed globally
# ==============================================================================


class TestApiAccessProfilesGlobal:
    def test_api_list_all_access_profiles(self, kronicle_rbac, test_zone, test_channel, test_row_id, profile_role):
        tag = tiny_id()
        zone_ap = KronicleZoneAccess(name=f"zone_ap_global_{tag}", role=profile_role, zone=test_zone)
        chan_ap = KronicleChannelAccess(name=f"chan_ap_global_{tag}", role=profile_role, channel=test_channel)
        row_ap = KronicleRowAccess(
            name=f"row_ap_global_{tag}",
            role=profile_role,
            row=KronicleRow(id=test_row_id, channel_id=test_channel.id),
        )
        created_zone = kronicle_rbac.create_zone_access_profile(zone_ap)
        created_chan = kronicle_rbac.create_channel_access_profile(chan_ap)
        created_row = kronicle_rbac.create_row_access_profile(row_ap)
        try:
            all_profiles = kronicle_rbac.list_access_profiles()
            assert isinstance(all_profiles, dict)
            assert "zone" in all_profiles
            assert "channel" in all_profiles
            assert "row" in all_profiles
            zone_ids = [p.id for p in all_profiles["zone"]]
            chan_ids = [p.id for p in all_profiles["channel"]]
            row_ids = [p.id for p in all_profiles["row"]]
            assert created_zone.id in zone_ids
            assert created_chan.id in chan_ids
            assert created_row.id in row_ids
        finally:
            kronicle_rbac.delete_zone_access_profile(profile_id=created_zone.id)
            kronicle_rbac.delete_channel_access_profile(profile_id=created_chan.id)
            kronicle_rbac.delete_row_access_profile(profile_id=created_row.id)


# ==============================================================================
# Nonexistent / edge cases
# ==============================================================================


class TestApiAccessProfileEdgeCases:
    def test_api_get_nonexistent_zone_access_profile(self, kronicle_rbac):
        fake_id = UUID("00000000-0000-0000-0000-000000000000")
        try:
            kronicle_rbac.get_zone_access_profile(profile_id=fake_id)
        except Exception:
            pass

    def test_api_get_nonexistent_channel_access_profile(self, kronicle_rbac):
        fake_id = UUID("00000000-0000-0000-0000-000000000000")
        try:
            kronicle_rbac.get_channel_access_profile(profile_id=fake_id)
        except Exception:
            pass

    def test_api_get_nonexistent_row_access_profile(self, kronicle_rbac):
        fake_id = UUID("00000000-0000-0000-0000-000000000000")
        try:
            kronicle_rbac.get_row_access_profile(profile_id=fake_id)
        except Exception:
            pass
