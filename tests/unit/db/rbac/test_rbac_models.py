# tests/unit/db/rbac/test_rbac_models.py
"""Tests for RBAC data models (namespace / table_args / snapshot / repr / declared attrs)."""

from uuid import uuid4

import pytest
from sqlalchemy.exc import SAWarning

from kronicle.db.rbac.links.group_hierarchy import RbacGroupHierarchy
from kronicle.db.rbac.links.rbac_policy import RbacPolicy
from kronicle.db.rbac.models.rbac_entity import RbacEntity
from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_role import RbacRole


def test_rbac_group_hierarchy_namespace():
    assert RbacGroupHierarchy.namespace() == "rbac"


def test_rbac_entity_namespace_and_table_args():
    assert RbacEntity.namespace() == "rbac"
    assert RbacEntity.table_args() == {"schema": "rbac"}


def test_rbac_group_snapshot():
    group = RbacGroup(id=uuid4(), name="admins")
    snap = group.snapshot
    assert snap["name"] == "admins"
    assert snap["user_ids"] == []
    assert isinstance(snap["id"], str)


def test_rbac_role_repr():
    role = RbacRole(name="Reader")
    assert repr(role) == "<Role Reader>"


def test_rbac_policy_access_profile_id_not_implemented():
    # SA warns about accessing a declared_attr on the non-mapped base, then the attr raises.
    with (
        pytest.warns(SAWarning, match="Unmanaged access"),
        pytest.raises(NotImplementedError, match="FK to its AccessProfile"),
    ):
        RbacPolicy.access_profile_id  # noqa: B018


def test_rbac_policy_access_profile_not_implemented():
    with (
        pytest.warns(SAWarning, match="Unmanaged access"),
        pytest.raises(NotImplementedError, match="relationship to AccessProfile"),
    ):
        RbacPolicy.access_profile  # noqa: B018
