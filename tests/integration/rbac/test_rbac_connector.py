# tests/integration/rbac/test_rbac_connector.py

import pytest
from kronicle_sdk.models.rbac.kronicle_user import KronicleUser
from kronicle_sdk.utils.log import log_d
from kronicle_sdk.utils.str_utils import tiny_id


@pytest.mark.integration
def test_get_all_users(kronicle_rbac, test_user):
    """Retrieve all users and inspect the test user."""
    here = "rbac_connector"
    usr_list = kronicle_rbac.get_all_users()
    log_d(here, f"Number of users: {len(usr_list)}")

    assert isinstance(usr_list, list)
    assert len(usr_list) > 0
    assert test_user.email in {u.email for u in usr_list}
    assert test_user.name in {u.name for u in usr_list}


@pytest.mark.integration
def test_get_user_by_email_and_name(kronicle_rbac, test_user):
    """Test getting a user by email and name, including a non-existent user."""
    by_email = kronicle_rbac.get_user_by(email=test_user.email)
    by_name = kronicle_rbac.get_user_by(name=test_user.name)
    by_fake = kronicle_rbac.get_user_by(name=f"{test_user.name}_nonexistent")

    log_d("get by email", by_email)
    log_d("get by name", by_name)
    log_d("get fake name", by_fake)

    assert by_email is not None
    assert by_name is not None
    assert by_fake is None


@pytest.mark.integration
def test_crud_user(kronicle_rbac):
    """Test creating, patching, and deleting a user."""
    here = "rbac_connector"
    tag = tiny_id()

    # Create user
    usr = KronicleUser(
        email=f"crud_{tag}@kronicle.app",
        name=f"crud_user_{tag}",
        password="CrudTest_789",
    )
    res = kronicle_rbac.create_user(usr)
    log_d(here, "Created", res)
    assert res is not None
    assert isinstance(res, KronicleUser)
    assert res.email == usr.email
    assert res.name == usr.name

    # Patch user (update fields) — reuse the created user's id to avoid "None" in payload
    patch = KronicleUser(
        id=res.id,
        email=res.email,
        name=f"{res.name}_patched",
        full_name="Patched Name",
    )
    kronicle_rbac.patch_user(patch)

    # Delete user (cleanup, regardless of patch outcome)
    usr = kronicle_rbac.deactivate_user(res)
    kronicle_rbac.remove_user_by_id(usr.id)
