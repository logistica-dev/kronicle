# tests/integration/rbac/test_rbac_api_reads.py
"""Exercises the RBAC read/check routes that the SDK exposes but tests didn't hit yet."""

import pytest
from kronicle_sdk.models.rbac.kronicle_role import KronicleRole
from kronicle_sdk.models.rbac.kronicle_user import KronicleUser
from kronicle_sdk.utils.log import log_d
from kronicle_sdk.utils.str_utils import tiny_id


@pytest.mark.integration
def test_api_get_user_by_id(kronicle_rbac, test_user):
    """GET /rbac/v1/users/{user_id}."""
    here = "rbac_reads"
    user = kronicle_rbac.get_user_by_id(user_id=test_user.id)
    log_d(here, "by_id", user)
    assert user is not None
    assert isinstance(user, KronicleUser)
    assert user.id == test_user.id
    assert user.email == test_user.email


@pytest.mark.integration
def test_api_get_role_by_id(kronicle_rbac):
    """GET /rbac/v1/roles/{role_id}."""
    tag = tiny_id()
    role = kronicle_rbac.create_role(KronicleRole(name=f"get_role_{tag}", permissions=["channel:read"]))
    try:
        fetched = kronicle_rbac.get_role_by_id(role_id=role.id)
        assert fetched is not None
        assert isinstance(fetched, KronicleRole)
        assert fetched.id == role.id
        assert fetched.name == role.name
    finally:
        kronicle_rbac.delete_role(role_id=role.id)


@pytest.mark.integration
def test_api_patch_role(kronicle_rbac):
    """PATCH /rbac/v1/roles/{role_id}."""
    tag = tiny_id()
    role = kronicle_rbac.create_role(KronicleRole(name=f"patch_role_{tag}", permissions=["channel:read"]))
    try:
        patch = KronicleRole(
            id=role.id,
            name=f"{role.name}_patched",
            permissions=["channel:read", "channel:update"],
        )
        patched = kronicle_rbac.patch_role(patch)
        assert patched is not None
        assert patched.name == f"{role.name}_patched"
        assert "channel:update" in patched.permissions
    finally:
        kronicle_rbac.delete_role(role_id=role.id)


@pytest.mark.integration
def test_api_check_user_has_role(kronicle_rbac, test_user):
    """GET /rbac/v1/users/{user_id}/roles/{role_id}."""
    tag = tiny_id()
    role = kronicle_rbac.create_role(KronicleRole(name=f"chk_usr_{tag}", permissions=["channel:read"]))
    try:
        assert not kronicle_rbac.check_user_has_role(user_id=test_user.id, role_id=role.id)
        kronicle_rbac.assign_role_to_user(role_id=role.id, user_id=test_user.id)
        res = kronicle_rbac.check_user_has_role(user_id=test_user.id, role_id=role.id)
        assert res is not None
        assert "user" in res and "role" in res
    finally:
        kronicle_rbac.delete_role(role_id=role.id, force=True)


@pytest.mark.integration
def test_api_check_group_has_role(kronicle_rbac, test_group):
    """GET /rbac/v1/groups/{group_id}/roles/{role_id}."""
    tag = tiny_id()
    role = kronicle_rbac.create_role(KronicleRole(name=f"chk_grp_{tag}", permissions=["channel:read"]))
    try:
        assert not kronicle_rbac.check_group_has_role(group_id=test_group.id, role_id=role.id)
        kronicle_rbac.assign_role_to_group(role_id=role.id, group_id=test_group.id)
        res = kronicle_rbac.check_group_has_role(group_id=test_group.id, role_id=role.id)
        assert res is not None
        assert "group" in res and "role" in res
    finally:
        kronicle_rbac.delete_role(role_id=role.id, force=True)


@pytest.mark.integration
def test_api_check_user_in_group(kronicle_rbac, test_user, test_group):
    """GET /rbac/v1/users/{user_id}/groups/{group_id}."""
    assert not kronicle_rbac.check_user_in_group(user_id=test_user.id, group_id=test_group.id)
    kronicle_rbac.add_user_to_group(group_id=test_group.id, user_id=test_user.id)
    try:
        res = kronicle_rbac.check_user_in_group(user_id=test_user.id, group_id=test_group.id)
        assert res is not None
        assert "user" in res and "group" in res
    finally:
        kronicle_rbac.remove_user_from_group(group_id=test_group.id, user_id=test_user.id)


@pytest.mark.integration
def test_api_get_users_from_group(kronicle_rbac, test_user, test_group):
    """GET /rbac/v1/groups/{group_id}/users."""
    users = kronicle_rbac.get_users_from_group(group_id=test_group.id)
    assert isinstance(users, list)
    assert str(test_user.id) not in [str(u.id) for u in users]
    kronicle_rbac.add_user_to_group(group_id=test_group.id, user_id=test_user.id)
    try:
        users = kronicle_rbac.get_users_from_group(group_id=test_group.id)
        assert str(test_user.id) in [str(u.id) for u in users]
    finally:
        kronicle_rbac.remove_user_from_group(group_id=test_group.id, user_id=test_user.id)


@pytest.mark.integration
def test_api_list_role_subjects(kronicle_rbac, test_user):
    """GET /rbac/v1/roles/{role_id}/subjects."""
    tag = tiny_id()
    role = kronicle_rbac.create_role(KronicleRole(name=f"subjects_{tag}", permissions=["channel:read"]))
    try:
        kronicle_rbac.assign_role_to_user(role_id=role.id, user_id=test_user.id)
        subjects = kronicle_rbac.list_role_subjects(role_id=role.id)
        assert isinstance(subjects, dict)
        assert str(test_user.id) in [u["id"] for u in subjects.get("users", [])]
    finally:
        kronicle_rbac.delete_role(role_id=role.id, force=True)


@pytest.mark.integration
def test_api_list_policies(kronicle_rbac):
    """GET /rbac/v1/policies returns all policies grouped by resource type."""
    policies = kronicle_rbac.list_policies()
    assert isinstance(policies, dict)
    for resource in ["zone", "channel", "row"]:
        assert resource in policies
        assert isinstance(policies[resource], list)
