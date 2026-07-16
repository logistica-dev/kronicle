# tests/integration/rbac/test_rbac_api_groups.py

import pytest
from kronicle_sdk.models.rbac.kronicle_group import KronicleGroup
from kronicle_sdk.utils.log import log_d
from kronicle_sdk.utils.str_utils import tiny_id


@pytest.mark.integration
def test_api_list_groups(kronicle_rbac, test_group):
    here = "rbac_groups"
    groups = kronicle_rbac.list_groups()
    log_d(here, f"Number of groups: {len(groups)}")
    assert isinstance(groups, list)
    assert len(groups) > 0
    assert test_group.name in {g.name for g in groups}


@pytest.mark.integration
def test_api_get_group(kronicle_rbac, test_group):
    group_id = test_group.id
    group = kronicle_rbac.get_group_by_id(group_id=group_id)
    assert group is not None
    assert group.id == group_id
    assert group.name == test_group.name


@pytest.mark.integration
def test_api_create_group(kronicle_rbac):
    here = "rbac_groups"
    tag = tiny_id()
    group = KronicleGroup(name=f"crud_group_{tag}", details={"test": True})
    created = kronicle_rbac.create_group(group)
    try:
        log_d(here, "Created", created)
        assert created is not None
        assert isinstance(created, KronicleGroup)
        assert created.name == group.name
    finally:
        kronicle_rbac.delete_group(group_id=created.id)


@pytest.mark.integration
def test_api_patch_group(kronicle_rbac):
    here = "rbac_groups"
    tag = tiny_id()
    group = KronicleGroup(name=f"patch_group_{tag}", details={"test": True})
    created = kronicle_rbac.create_group(group)
    try:
        patch = KronicleGroup(
            id=created.id,
            name=f"{created.name}_patched",
            details=created.details,
        )
        patched = kronicle_rbac.patch_group(group=patch)
        log_d(here, "Patched", patched)
        assert patched.name == patch.name
    finally:
        kronicle_rbac.delete_group(group_id=created.id)


@pytest.mark.integration
def test_api_delete_group(kronicle_rbac):
    tag = tiny_id()
    group = KronicleGroup(name=f"del_group_{tag}", details={"test": True})
    created = kronicle_rbac.create_group(group)
    deleted = kronicle_rbac.delete_group(group_id=created.id)
    assert deleted is not None
    groups = kronicle_rbac.list_groups()
    assert created.id not in {g.id for g in groups}


@pytest.mark.integration
def test_api_add_and_remove_user_from_group(kronicle_rbac, test_group, test_user):
    group_id = test_group.id
    user_id = test_user.id
    result = kronicle_rbac.add_user_to_group(group_id=group_id, user_id=user_id)
    assert result is not None
    result = kronicle_rbac.remove_user_from_group(group_id=group_id, user_id=user_id)
    assert result is not None
