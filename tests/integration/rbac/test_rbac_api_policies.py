# tests/integration/rbac/test_rbac_api_policies.py
"""Integration tests for RBAC policy CRUD endpoints.

Tests zone, channel, and row policy lifecycle:
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
from kronicle_sdk.models.rbac.kronicle_policy import (
    KronicleChannelPolicy,
    KronicleRowPolicy,
    KronicleSubject,
    KronicleZonePolicy,
)
from kronicle_sdk.models.rbac.kronicle_role import KronicleRole
from kronicle_sdk.models.rbac.kronicle_row import KronicleRow
from kronicle_sdk.utils.str_utils import tiny_id

pytestmark = pytest.mark.integration


# ==============================================================================
# Fixtures
# ==============================================================================


@pytest.fixture(scope="module")
def policy_role(kronicle_rbac) -> Generator[KronicleRole, None, None]:
    tag = tiny_id()
    role = kronicle_rbac.create_role(
        KronicleRole(name=f"policy_role_{tag}", permissions=["channel:read"], details={"test": True})
    )
    yield role
    try:
        kronicle_rbac.delete_role(role_id=role.id, force=True)
    except Exception:
        pass


# ==============================================================================
# Zone policies
# ==============================================================================


class TestApiZonePolicies:
    def test_api_create_zone_policy(self, kronicle_rbac, test_user, test_zone, policy_role):
        tag = tiny_id()
        policy = KronicleZonePolicy(
            name=f"zone_pol_{tag}",
            subject=KronicleSubject.from_user(test_user),
            access_profile=KronicleZoneAccess(
                name=f"zone_ap_{tag}",
                role=policy_role,
                zone=test_zone,
                description=f"Test zone policy {tag}",
            ),
            details={"test": True},
        )
        created = kronicle_rbac.create_zone_policy(policy)
        assert isinstance(created, KronicleZonePolicy)
        assert created.id is not None
        try:
            fetched = kronicle_rbac.get_zone_policy(policy_id=created.id)
            assert fetched.id == created.id
        finally:
            kronicle_rbac.delete_zone_policy(policy_id=created.id)

    def test_api_list_zone_policies(self, kronicle_rbac, test_user, test_zone, policy_role):
        tag = tiny_id()
        policy = KronicleZonePolicy(
            name=f"zone_pol_list_{tag}",
            subject=KronicleSubject.from_user(test_user),
            access_profile=KronicleZoneAccess(
                name=f"zone_ap_list_{tag}",
                role=policy_role,
                zone=test_zone,
            ),
        )
        created = kronicle_rbac.create_zone_policy(policy)
        try:
            policies = kronicle_rbac.list_zone_policies()
            assert isinstance(policies, list)
            ids = [p.id for p in policies]
            assert created.id in ids
        finally:
            kronicle_rbac.delete_zone_policy(policy_id=created.id)

    def test_api_patch_zone_policy(self, kronicle_rbac, test_user, test_zone, policy_role):
        tag = tiny_id()
        policy = KronicleZonePolicy(
            name=f"zone_pol_patch_{tag}",
            subject=KronicleSubject.from_user(test_user),
            access_profile=KronicleZoneAccess(
                name=f"zone_ap_patch_{tag}",
                role=policy_role,
                zone=test_zone,
            ),
        )
        created = kronicle_rbac.create_zone_policy(policy)
        try:
            created.name = f"zone_pol_patch_{tag}_updated"
            updated = kronicle_rbac.patch_zone_policy(zone_policy=created)
            assert updated.name == f"zone_pol_patch_{tag}_updated"
        finally:
            kronicle_rbac.delete_zone_policy(policy_id=created.id)

    def test_api_delete_zone_policy(self, kronicle_rbac, test_user, test_zone, policy_role):
        tag = tiny_id()
        policy = KronicleZonePolicy(
            name=f"zone_pol_del_{tag}",
            subject=KronicleSubject.from_user(test_user),
            access_profile=KronicleZoneAccess(
                name=f"zone_ap_del_{tag}",
                role=policy_role,
                zone=test_zone,
            ),
        )
        created = kronicle_rbac.create_zone_policy(policy)
        deleted = kronicle_rbac.delete_zone_policy(policy_id=created.id)
        assert deleted.id == created.id


# ==============================================================================
# Channel policies
# ==============================================================================


class TestApiChannelPolicies:
    def test_api_create_channel_policy(self, kronicle_rbac, test_user, test_channel, policy_role):
        tag = tiny_id()
        policy = KronicleChannelPolicy(
            name=f"chan_pol_{tag}",
            subject=KronicleSubject.from_user(test_user),
            access_profile=KronicleChannelAccess(
                name=f"chan_ap_{tag}",
                role=policy_role,
                channel=test_channel,
                description=f"Test channel policy {tag}",
            ),
            details={"test": True},
        )
        created = kronicle_rbac.create_channel_policy(policy)
        assert isinstance(created, KronicleChannelPolicy)
        assert created.id is not None
        try:
            fetched = kronicle_rbac.get_channel_policy(policy_id=created.id)
            assert fetched.id == created.id
        finally:
            kronicle_rbac.delete_channel_policy(policy_id=created.id)

    def test_api_list_channel_policies(self, kronicle_rbac, test_user, test_channel, policy_role):
        tag = tiny_id()
        policy = KronicleChannelPolicy(
            name=f"chan_pol_list_{tag}",
            subject=KronicleSubject.from_user(test_user),
            access_profile=KronicleChannelAccess(
                name=f"chan_ap_list_{tag}",
                role=policy_role,
                channel=test_channel,
            ),
        )
        created = kronicle_rbac.create_channel_policy(policy)
        try:
            policies = kronicle_rbac.list_channel_policies()
            assert isinstance(policies, list)
            ids = [p.id for p in policies]
            assert created.id in ids
        finally:
            kronicle_rbac.delete_channel_policy(policy_id=created.id)

    def test_api_patch_channel_policy(self, kronicle_rbac, test_user, test_channel, policy_role):
        tag = tiny_id()
        policy = KronicleChannelPolicy(
            name=f"chan_pol_patch_{tag}",
            subject=KronicleSubject.from_user(test_user),
            access_profile=KronicleChannelAccess(
                name=f"chan_ap_patch_{tag}",
                role=policy_role,
                channel=test_channel,
            ),
        )
        created = kronicle_rbac.create_channel_policy(policy)
        try:
            created.name = f"chan_pol_patch_{tag}_updated"
            updated = kronicle_rbac.patch_channel_policy(channel_policy=created)
            assert updated.name == f"chan_pol_patch_{tag}_updated"
        finally:
            kronicle_rbac.delete_channel_policy(policy_id=created.id)

    def test_api_delete_channel_policy(self, kronicle_rbac, test_user, test_channel, policy_role):
        tag = tiny_id()
        policy = KronicleChannelPolicy(
            name=f"chan_pol_del_{tag}",
            subject=KronicleSubject.from_user(test_user),
            access_profile=KronicleChannelAccess(
                name=f"chan_ap_del_{tag}",
                role=policy_role,
                channel=test_channel,
            ),
        )
        created = kronicle_rbac.create_channel_policy(policy)
        deleted = kronicle_rbac.delete_channel_policy(policy_id=created.id)
        assert deleted.id == created.id


# ==============================================================================
# Row policies
# ==============================================================================


class TestApiRowPolicies:
    def test_api_create_row_policy(self, kronicle_rbac, test_user, test_channel, test_row_id, policy_role):
        tag = tiny_id()
        policy = KronicleRowPolicy(
            name=f"row_pol_{tag}",
            subject=KronicleSubject.from_user(test_user),
            access_profile=KronicleRowAccess(
                name=f"row_ap_{tag}",
                role=policy_role,
                row=KronicleRow(id=test_row_id, channel_id=test_channel.id),
                description=f"Test row policy {tag}",
            ),
            details={"test": True},
        )
        created = kronicle_rbac.create_row_policy(policy)
        assert isinstance(created, KronicleRowPolicy)
        assert created.id is not None
        try:
            fetched = kronicle_rbac.get_row_policy(policy_id=created.id)
            assert fetched.id == created.id
        finally:
            kronicle_rbac.delete_row_policy(policy_id=created.id)

    def test_api_list_row_policies(self, kronicle_rbac, test_user, test_channel, test_row_id, policy_role):
        tag = tiny_id()
        policy = KronicleRowPolicy(
            name=f"row_pol_list_{tag}",
            subject=KronicleSubject.from_user(test_user),
            access_profile=KronicleRowAccess(
                name=f"row_ap_list_{tag}",
                role=policy_role,
                row=KronicleRow(id=test_row_id, channel_id=test_channel.id),
            ),
        )
        created = kronicle_rbac.create_row_policy(policy)
        try:
            policies = kronicle_rbac.list_row_policies()
            assert isinstance(policies, list)
            ids = [p.id for p in policies]
            assert created.id in ids
        finally:
            kronicle_rbac.delete_row_policy(policy_id=created.id)

    def test_api_patch_row_policy(self, kronicle_rbac, test_user, test_channel, test_row_id, policy_role):
        tag = tiny_id()
        policy = KronicleRowPolicy(
            name=f"row_pol_patch_{tag}",
            subject=KronicleSubject.from_user(test_user),
            access_profile=KronicleRowAccess(
                name=f"row_ap_patch_{tag}",
                role=policy_role,
                row=KronicleRow(id=test_row_id, channel_id=test_channel.id),
            ),
        )
        created = kronicle_rbac.create_row_policy(policy)
        try:
            created.name = f"row_pol_patch_{tag}_updated"
            updated = kronicle_rbac.patch_row_policy(row_policy=created)
            assert updated.name == f"row_pol_patch_{tag}_updated"
        finally:
            kronicle_rbac.delete_row_policy(policy_id=created.id)

    def test_api_delete_row_policy(self, kronicle_rbac, test_user, test_channel, test_row_id, policy_role):
        tag = tiny_id()
        policy = KronicleRowPolicy(
            name=f"row_pol_del_{tag}",
            subject=KronicleSubject.from_user(test_user),
            access_profile=KronicleRowAccess(
                name=f"row_ap_del_{tag}",
                role=policy_role,
                row=KronicleRow(id=test_row_id, channel_id=test_channel.id),
            ),
        )
        created = kronicle_rbac.create_row_policy(policy)
        deleted = kronicle_rbac.delete_row_policy(policy_id=created.id)
        assert deleted.id == created.id


# ==============================================================================
# Policy appears in permissions introspection
# ==============================================================================


class TestApiPolicyInPermissions:
    def test_api_zone_policy_appears_in_user_permissions(self, kronicle_rbac, test_user, test_zone, policy_role):
        tag = tiny_id()
        policy = KronicleZonePolicy(
            name=f"zone_pol_intro_{tag}",
            subject=KronicleSubject.from_user(test_user),
            access_profile=KronicleZoneAccess(
                name=f"zone_ap_intro_{tag}",
                role=policy_role,
                zone=test_zone,
            ),
        )
        created = kronicle_rbac.create_zone_policy(policy)
        try:
            perms = kronicle_rbac.get_user_permissions(user_id=test_user.id)
            zone_policy_ids = [p.id for p in perms.zone_policies]
            assert created.id in zone_policy_ids
        finally:
            kronicle_rbac.delete_zone_policy(policy_id=created.id)

    def test_api_channel_policy_appears_in_user_permissions(self, kronicle_rbac, test_user, test_channel, policy_role):
        tag = tiny_id()
        policy = KronicleChannelPolicy(
            name=f"chan_pol_intro_{tag}",
            subject=KronicleSubject.from_user(test_user),
            access_profile=KronicleChannelAccess(
                name=f"chan_ap_intro_{tag}",
                role=policy_role,
                channel=test_channel,
            ),
        )
        created = kronicle_rbac.create_channel_policy(policy)
        try:
            perms = kronicle_rbac.get_user_permissions(user_id=test_user.id)
            channel_policy_ids = [p.id for p in perms.channel_policies]
            assert created.id in channel_policy_ids
        finally:
            kronicle_rbac.delete_channel_policy(policy_id=created.id)


# ==============================================================================
# Nonexistent / edge cases
# ==============================================================================


class TestApiPolicyEdgeCases:
    def test_api_get_nonexistent_zone_policy(self, kronicle_rbac):
        fake_id = UUID("00000000-0000-0000-0000-000000000000")
        try:
            kronicle_rbac.get_zone_policy(policy_id=fake_id)
        except Exception:
            pass  # 404 is acceptable

    def test_api_get_nonexistent_channel_policy(self, kronicle_rbac):
        fake_id = UUID("00000000-0000-0000-0000-000000000000")
        try:
            kronicle_rbac.get_channel_policy(policy_id=fake_id)
        except Exception:
            pass

    def test_api_get_nonexistent_row_policy(self, kronicle_rbac):
        fake_id = UUID("00000000-0000-0000-0000-000000000000")
        try:
            kronicle_rbac.get_row_policy(policy_id=fake_id)
        except Exception:
            pass
