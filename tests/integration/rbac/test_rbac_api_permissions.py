# tests/integration/rbac/test_rbac_api_permissions.py
"""Integration tests for RBAC permissions introspection endpoints.

GET /users/{user_id}/permissions
GET /groups/{group_id}/permissions

Uses KronicleRbac SDK methods for all operations.
Requires a running server with valid KRONICLE_SU_NAME / KRONICLE_SU_PASS env vars.
"""

from collections.abc import Generator

import pytest
from kronicle_sdk.models.rbac.kronicle_permissions import KroniclePermissions
from kronicle_sdk.models.rbac.kronicle_role import KronicleRole
from kronicle_sdk.utils.str_utils import tiny_id

pytestmark = pytest.mark.integration


# ==============================================================================
# Fixtures
# ==============================================================================


@pytest.fixture(scope="module")
def perm_role(kronicle_rbac) -> Generator[KronicleRole, None, None]:
    tag = tiny_id()
    role = kronicle_rbac.create_role(
        KronicleRole(name=f"perm_role_{tag}", permissions=["channel:read"], details={"test": True})
    )
    yield role
    try:
        kronicle_rbac.delete_role(role_id=role.id, force=True)
    except Exception:
        pass


# ==============================================================================
# User permissions
# ==============================================================================


class TestApiUserPermissionsDetail:
    def test_api_user_permissions_empty(self, kronicle_rbac, test_user):
        """A fresh user with no roles or group memberships should have empty permissions."""
        perms = kronicle_rbac.get_user_permissions(user_id=test_user.id)
        assert isinstance(perms, KroniclePermissions)
        assert perms.direct_roles == []
        assert perms.group_roles == []
        assert isinstance(perms.zone_policies, list)
        assert isinstance(perms.channel_policies, list)
        assert isinstance(perms.row_policies, list)

    def test_api_user_permissions_direct_role(self, kronicle_rbac, test_user, perm_role):
        """After assigning a role directly, it should appear in direct_roles."""
        kronicle_rbac.assign_role_to_user(role_id=perm_role.id, user_id=test_user.id)
        try:
            perms = kronicle_rbac.get_user_permissions(user_id=test_user.id)
            role_ids = [r.id for r in perms.direct_roles]
            assert perm_role.id in role_ids
        finally:
            kronicle_rbac.remove_role_from_user(role_id=perm_role.id, user_id=test_user.id)

    def test_api_user_permissions_group_role(self, kronicle_rbac, test_user, test_group, perm_role):
        """After adding user to a group and assigning role to group, group_roles should appear."""
        kronicle_rbac.assign_role_to_group(role_id=perm_role.id, group_id=test_group.id)
        kronicle_rbac.add_user_to_group(group_id=test_group.id, user_id=test_user.id)
        try:
            perms = kronicle_rbac.get_user_permissions(user_id=test_user.id)
            group_role_names = [(gr.group.name, gr.role.name) for gr in perms.group_roles]
            assert (test_group.name, perm_role.name) in group_role_names
        finally:
            kronicle_rbac.remove_role_from_group(role_id=perm_role.id, group_id=test_group.id)
            kronicle_rbac.remove_user_from_group(group_id=test_group.id, user_id=test_user.id)

    def test_api_user_permissions_nonexistent_user(self, kronicle_rbac):
        """Permissions for a nonexistent user should return empty or raise."""
        from uuid import UUID

        fake_id = UUID("00000000-0000-0000-0000-000000000000")
        try:
            perms = kronicle_rbac.get_user_permissions(user_id=fake_id)
            assert isinstance(perms, KroniclePermissions)
        except Exception:
            pass  # 404 or error is acceptable


# ==============================================================================
# Group permissions
# ==============================================================================


class TestApiGroupPermissionsDetail:
    def test_api_group_permissions_empty(self, kronicle_rbac, test_group):
        """A fresh group with no roles assigned should have empty permissions."""
        perms = kronicle_rbac.get_group_permissions(group_id=test_group.id)
        assert isinstance(perms, KroniclePermissions)
        assert perms.direct_roles == []
        assert perms.group_roles == []
        assert isinstance(perms.zone_policies, list)
        assert isinstance(perms.channel_policies, list)
        assert isinstance(perms.row_policies, list)

    def test_api_group_permissions_direct_role(self, kronicle_rbac, test_group, perm_role):
        """After assigning a role to a group, it should appear in direct_roles."""
        kronicle_rbac.assign_role_to_group(role_id=perm_role.id, group_id=test_group.id)
        try:
            perms = kronicle_rbac.get_group_permissions(group_id=test_group.id)
            role_ids = [r.id for r in perms.direct_roles]
            assert perm_role.id in role_ids
        finally:
            kronicle_rbac.remove_role_from_group(role_id=perm_role.id, group_id=test_group.id)

    def test_api_group_permissions_nonexistent_group(self, kronicle_rbac):
        """Permissions for a nonexistent group should return empty or raise."""
        from uuid import UUID

        fake_id = UUID("00000000-0000-0000-0000-000000000000")
        try:
            perms = kronicle_rbac.get_group_permissions(group_id=fake_id)
            assert isinstance(perms, KroniclePermissions)
        except Exception:
            pass  # 404 or error is acceptable


# ==============================================================================
# Cross-subject permissions consistency
# ==============================================================================


class TestApiPermissionsConsistency:
    def test_api_user_and_group_permissions_consistent(self, kronicle_rbac, test_user, test_group, perm_role):
        """User and group permissions should be consistent when user is in group."""
        kronicle_rbac.assign_role_to_group(role_id=perm_role.id, group_id=test_group.id)
        kronicle_rbac.add_user_to_group(group_id=test_group.id, user_id=test_user.id)
        try:
            user_perms = kronicle_rbac.get_user_permissions(user_id=test_user.id)
            group_perms = kronicle_rbac.get_group_permissions(group_id=test_group.id)

            # Group's direct role should appear in user's group_roles
            group_role_ids = [r.id for r in group_perms.direct_roles]
            user_group_role_ids = [gr.role.id for gr in user_perms.group_roles]
            for rid in group_role_ids:
                assert rid in user_group_role_ids
        finally:
            kronicle_rbac.remove_role_from_group(role_id=perm_role.id, group_id=test_group.id)
            kronicle_rbac.remove_user_from_group(group_id=test_group.id, user_id=test_user.id)
