# tests/integration/rbac/test_rbac_api_connector.py

import pytest
from kronicle_sdk.models.rbac.kronicle_role import KronicleRole
from kronicle_sdk.models.rbac.kronicle_user import KronicleUser
from kronicle_sdk.utils.log import log_d
from kronicle_sdk.utils.str_utils import tiny_id


@pytest.mark.integration
def test_api_list_users(kronicle_rbac, test_user):
    """Retrieve all users and inspect the test user."""
    here = "rbac_connector"
    usr_list = kronicle_rbac.list_users()
    log_d(here, f"Number of users: {len(usr_list)}")

    assert isinstance(usr_list, list)
    assert len(usr_list) > 0
    assert test_user.email in {u.email for u in usr_list}
    assert test_user.name in {u.name for u in usr_list}


@pytest.mark.integration
def test_api_get_user_by_email_and_name(kronicle_rbac, test_user):
    """Test getting a user by email and name, including a non-existent user."""
    by_email = kronicle_rbac.get_user_by_email(email=test_user.email)
    by_name = kronicle_rbac.get_user_by_name(name=test_user.name)
    by_fake = kronicle_rbac.get_user_by_name(name=f"{test_user.name}_nonexistent")

    log_d("get by email", by_email)
    log_d("get by name", by_name)
    log_d("get fake name", by_fake)

    assert by_email is not None
    assert by_name is not None
    assert by_fake is None


@pytest.mark.integration
def test_api_crud_user(kronicle_rbac):
    """Test creating, patching, and deleting a user."""
    here = "rbac_connector"
    tag = tiny_id()

    # Create user
    usr = KronicleUser(
        email=f"crud_{tag}@kronicle.app",
        name=f"crud_user_{tag}",
        password="CrudTest_789",
        details={"test": True},
    )
    res = kronicle_rbac.create_user(usr)
    log_d(here, "Created", res)
    assert res is not None
    assert isinstance(res, KronicleUser)
    assert res.email == usr.email
    assert res.name == usr.name

    try:
        # Patch user (update fields) — reuse the created user's id to avoid "None" in payload
        patch = KronicleUser(
            id=res.id,
            email=res.email,
            name=f"{res.name}_patched",
            full_name="Patched Name",
        )
        kronicle_rbac.patch_user(user=patch)
    finally:
        # Delete user (cleanup, regardless of patch outcome)
        usr = kronicle_rbac.deactivate_user(user_id=res.id)
        kronicle_rbac.delete_user(user_id=usr.id)


@pytest.mark.integration
def test_api_get_users_for_role_direct(kronicle_rbac, test_user):
    """get_users_for_role returns users directly assigned to a role."""
    tag = tiny_id()
    role = kronicle_rbac.create_role(
        KronicleRole(name=f"test_usr_role_{tag}", permissions=["channel:read"], details={"test": True})
    )
    try:
        kronicle_rbac.assign_role_to_user(role_id=role.id, user_id=test_user.id)
        users = kronicle_rbac.get_users_for_role(role_id=role.id)
        assert str(test_user.id) in users
    finally:
        kronicle_rbac.delete_role(role_id=role.id, force=True)


@pytest.mark.integration
def test_api_get_users_for_role_indirect(kronicle_rbac, test_user, test_group):
    """get_users_for_role with indirect=True includes users via group membership."""
    tag = tiny_id()
    role = kronicle_rbac.create_role(
        KronicleRole(name=f"test_usr_role_ind_{tag}", permissions=["channel:read"], details={"test": True})
    )
    try:
        kronicle_rbac.add_user_to_group(group_id=test_group.id, user_id=test_user.id)
        kronicle_rbac.assign_role_to_group(role_id=role.id, group_id=test_group.id)
        direct_users = kronicle_rbac.get_users_for_role(role_id=role.id, indirect=False)
        assert str(test_user.id) not in direct_users
        all_users = kronicle_rbac.get_users_for_role(role_id=role.id, indirect=True)
        assert str(test_user.id) in all_users
    finally:
        try:
            kronicle_rbac.remove_user_from_group(group_id=test_group.id, user_id=test_user.id)
        except Exception:
            pass
        kronicle_rbac.delete_role(role_id=role.id, force=True)


@pytest.mark.integration
def test_api_get_groups_for_role_direct(kronicle_rbac, test_group):
    """get_groups_for_role returns groups directly assigned to a role."""
    tag = tiny_id()
    role = kronicle_rbac.create_role(
        KronicleRole(name=f"test_grp_role_{tag}", permissions=["channel:read"], details={"test": True})
    )
    try:
        kronicle_rbac.assign_role_to_group(role_id=role.id, group_id=test_group.id)
        groups = kronicle_rbac.get_groups_for_role(role_id=role.id)
        assert str(test_group.id) in groups
    finally:
        kronicle_rbac.delete_role(role_id=role.id, force=True)


@pytest.mark.integration
def test_api_get_users_for_role_empty(kronicle_rbac):
    """get_users_for_role returns empty list when no users are assigned."""
    tag = tiny_id()
    role = kronicle_rbac.create_role(
        KronicleRole(name=f"test_empty_role_{tag}", permissions=["channel:read"], details={"test": True})
    )
    try:
        users = kronicle_rbac.get_users_for_role(role_id=role.id)
        assert users == []
        groups = kronicle_rbac.get_groups_for_role(role_id=role.id)
        assert groups == []
    finally:
        kronicle_rbac.delete_role(role_id=role.id)
