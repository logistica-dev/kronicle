# tests/integration/rbac/test_rbac_connector.py

import pytest
from kronicle_sdk.conf.read_conf import Settings
from kronicle_sdk.connectors.rbac.rbac_connector import KronicleRbacConnector
from kronicle_sdk.models.rbac.kronicle_user import KronicleUser
from kronicle_sdk.utils.log import log_d, log_w


@pytest.fixture(scope="session")
def kronicle_rbac():
    """Return a connected KronicleRbacConnector instance."""
    co = Settings().connection
    connector = KronicleRbacConnector(co.url, co.usr, co.pwd)
    return connector


@pytest.mark.integration
def test_get_all_users(kronicle_rbac):
    """Retrieve all users and inspect the first user."""
    here = "rbac_connector"
    usr_list = kronicle_rbac.get_all_users()
    log_d(here, f"Number of users: {len(usr_list)}")
    for usr in usr_list:
        log_d(here, usr)

    assert isinstance(usr_list, list)
    assert len(usr_list) > 0

    usr1 = usr_list[0]
    log_d(here, "usr1 obj", usr1)
    log_d(here, "usr1.email", usr1.email)
    assert hasattr(usr1, "email")
    assert hasattr(usr1, "name")


@pytest.mark.integration
def test_get_user_by_email_and_name(kronicle_rbac):
    """Test getting a user by email and name, including a non-existent user."""
    usr_list = kronicle_rbac.get_all_users()
    usr1 = usr_list[0]

    by_email = kronicle_rbac.get_user_by(email=usr1.email)
    by_name = kronicle_rbac.get_user_by(name=usr1.name)
    by_fake = kronicle_rbac.get_user_by(name=f"{usr1.name}3")

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

    # Create user
    usr2 = KronicleUser(
        email="dave@toto.fr",
        name="Dave",
        orcid="1234-5678-9101",
        full_name="Dave Bond",
        password="Wonderful_Secrets_123403657",
    )
    try:
        res = kronicle_rbac.create_user(usr2)
        log_d(here, "Created", res)
        assert res is not None
        assert isinstance(res, KronicleUser)
    except Exception as e:
        log_w(here, e)

    # Patch user (update fields)
    usr2_patch = KronicleUser(
        email="dave@toto.fr",
        name="Dave2",
        orcid="1234-5678-9102",
        full_name="Dave Bond II",
        # password intentionally omitted
    )
    res_patch = kronicle_rbac.patch_user(usr2_patch)
    log_d(here, "Updated", res_patch)
    assert res_patch is not None
    assert isinstance(res_patch, KronicleUser)

    # Delete user
    res_delete = kronicle_rbac.delete_user(res_patch)
    log_d(here, "Deleted", res_delete)
    assert res_delete is not None
    assert isinstance(res_delete, KronicleUser)
