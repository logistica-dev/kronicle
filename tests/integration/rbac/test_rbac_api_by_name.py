# tests/integration/rbac/test_rbac_api_by_name.py
"""Integration tests for the ``?name=`` lookup of policies/access profiles/roles.

Each list endpoint (zone/channel/row access profiles and policies, plus roles) accepts an
optional ``name`` query parameter that returns the single matching record (or None) instead
of the full list. These tests exercise that surface via the SDK ``get_*_by_name`` methods.

Requires a running server with valid KRONICLE_SU_NAME / KRONICLE_SU_PASS env vars.
"""

from collections.abc import Generator

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


@pytest.fixture(scope="module")
def by_name_role(kronicle_rbac) -> Generator[KronicleRole, None, None]:
    tag = tiny_id()
    role = kronicle_rbac.create_role(
        KronicleRole(name=f"by_name_role_{tag}", permissions=["channel:read"], details={"test": True})
    )
    yield role
    try:
        kronicle_rbac.delete_role(role_id=role.id, force=True)
    except Exception:
        pass


# ==============================================================================
# Access profiles by name
# ==============================================================================


class TestApiAccessProfileByName:
    def test_found_zone(self, kronicle_rbac, test_zone, by_name_role):
        tag = tiny_id()
        access = KronicleZoneAccess(name=f"zap_by_name_{tag}", role=by_name_role, zone=test_zone)
        created = kronicle_rbac.create_zone_access_profile(access)
        try:
            found = kronicle_rbac.get_zone_access_profile_by_name(name=access.name)
            assert found is not None
            assert found.id == created.id
        finally:
            kronicle_rbac.delete_zone_access_profile(profile_id=created.id)

    def test_found_channel(self, kronicle_rbac, test_channel, by_name_role):
        tag = tiny_id()
        access = KronicleChannelAccess(name=f"cap_by_name_{tag}", role=by_name_role, channel=test_channel)
        created = kronicle_rbac.create_channel_access_profile(access)
        try:
            found = kronicle_rbac.get_channel_access_profile_by_name(name=access.name)
            assert found is not None
            assert found.id == created.id
        finally:
            kronicle_rbac.delete_channel_access_profile(profile_id=created.id)

    def test_found_row(self, kronicle_rbac, test_channel, test_row_id, by_name_role):
        tag = tiny_id()
        access = KronicleRowAccess(
            name=f"rap_by_name_{tag}",
            role=by_name_role,
            row=KronicleRow(id=test_row_id, channel_id=test_channel.id),
        )
        created = kronicle_rbac.create_row_access_profile(access)
        try:
            found = kronicle_rbac.get_row_access_profile_by_name(name=access.name)
            assert found is not None
            assert found.id == created.id
        finally:
            kronicle_rbac.delete_row_access_profile(profile_id=created.id)

    def test_missing_returns_none(self, kronicle_rbac):
        missing = f"no_such_profile_{tiny_id()}"
        assert kronicle_rbac.get_zone_access_profile_by_name(name=missing) is None
        assert kronicle_rbac.get_channel_access_profile_by_name(name=missing) is None
        assert kronicle_rbac.get_row_access_profile_by_name(name=missing) is None


# ==============================================================================
# Policies by name
# ==============================================================================


class TestApiPolicyByName:
    def test_found_zone(self, kronicle_rbac, test_user, test_zone, by_name_role):
        tag = tiny_id()
        policy = KronicleZonePolicy(
            name=f"zone_pol_by_name_{tag}",
            subject=KronicleSubject.from_user(test_user),
            access_profile=KronicleZoneAccess(name=f"zap_bn_{tag}", role=by_name_role, zone=test_zone),
        )
        created = kronicle_rbac.create_zone_policy(policy)
        try:
            found = kronicle_rbac.get_zone_policy_by_name(name=created.name)
            assert found is not None
            assert found.id == created.id
        finally:
            kronicle_rbac.delete_zone_policy(policy_id=created.id)

    def test_found_channel(self, kronicle_rbac, test_user, test_channel, by_name_role):
        tag = tiny_id()
        policy = KronicleChannelPolicy(
            name=f"chan_pol_by_name_{tag}",
            subject=KronicleSubject.from_user(test_user),
            access_profile=KronicleChannelAccess(name=f"cap_bn_{tag}", role=by_name_role, channel=test_channel),
        )
        created = kronicle_rbac.create_channel_policy(policy)
        try:
            found = kronicle_rbac.get_channel_policy_by_name(name=created.name)
            assert found is not None
            assert found.id == created.id
        finally:
            kronicle_rbac.delete_channel_policy(policy_id=created.id)

    def test_found_row(self, kronicle_rbac, test_user, test_channel, test_row_id, by_name_role):
        tag = tiny_id()
        policy = KronicleRowPolicy(
            name=f"row_pol_by_name_{tag}",
            subject=KronicleSubject.from_user(test_user),
            access_profile=KronicleRowAccess(
                name=f"rap_bn_{tag}",
                role=by_name_role,
                row=KronicleRow(id=test_row_id, channel_id=test_channel.id),
            ),
        )
        created = kronicle_rbac.create_row_policy(policy)
        try:
            found = kronicle_rbac.get_row_policy_by_name(name=created.name)
            assert found is not None
            assert found.id == created.id
        finally:
            kronicle_rbac.delete_row_policy(policy_id=created.id)

    def test_missing_returns_none(self, kronicle_rbac):
        missing = f"no_such_policy_{tiny_id()}"
        assert kronicle_rbac.get_zone_policy_by_name(name=missing) is None
        assert kronicle_rbac.get_channel_policy_by_name(name=missing) is None
        assert kronicle_rbac.get_row_policy_by_name(name=missing) is None


# ==============================================================================
# Roles by name
# ==============================================================================


class TestApiRoleByName:
    def test_found(self, kronicle_rbac):
        tag = tiny_id()
        role = kronicle_rbac.create_role(
            KronicleRole(name=f"role_by_name_{tag}", permissions=["channel:read"], details={"test": True})
        )
        try:
            found = kronicle_rbac.get_role_by_name(name=role.name)
            assert found is not None
            assert found.id == role.id
        finally:
            kronicle_rbac.delete_role(role_id=role.id, force=True)

    def test_missing_returns_none(self, kronicle_rbac):
        assert kronicle_rbac.get_role_by_name(name=f"no_such_role_{tiny_id()}") is None
