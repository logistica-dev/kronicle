# tests/unit/services/conftest.py
"""Shared fixtures and factory helpers for RBAC service unit tests."""

from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from kronicle.schemas.core.safe_ressource_schema import OutputCoreChannel
from kronicle.services.rbac_service import RbacService


@pytest.fixture
def mock_db_session():
    mock_session = MagicMock()
    mock_session.get_db.return_value.__enter__.return_value = MagicMock()
    mock_session.transaction.return_value.__enter__.return_value = MagicMock()
    return mock_session


@pytest.fixture
def rbac_service(mock_db_session):
    return RbacService(rbac_db_session=mock_db_session)


def fake_user(id=None, name="usr", email="u@k.app"):
    u = MagicMock()
    u.id = id or uuid4()
    u.name = name
    u.email = email
    u.external_id = None
    u.full_name = None
    u.details = {}
    u.is_active = True
    u.is_superuser = False
    u.password_hash = "h"
    u.snapshot = {"id": str(u.id), "email": email, "name": name, "details": {}}
    return u


def fake_role(id=None, name="role"):
    r = MagicMock()
    r.id = id or uuid4()
    r.name = name
    r.description = ""
    r.permissions = []
    r.restrictions = []
    r.details = {}
    r.snapshot = {"id": str(r.id), "name": name, "permissions": [], "restrictions": [], "is_global": False}
    return r


def fake_group(id=None, name="grp"):
    g = MagicMock()
    g.id = id or uuid4()
    g.name = name
    g.details = {}
    g.snapshot = {"id": str(g.id), "name": name}
    return g


def fake_user_role_link(user=None, role=None):
    """Build a MagicMock that mimics an RbacUserRoles link for OutputUserRole.from_db."""
    link = MagicMock()
    link.id = uuid4()
    link.name = None
    link.details = None
    link.user = user or fake_user()
    link.role = role or fake_role()
    return link


def fake_group_role_link(group=None, role=None):
    """Build a MagicMock that mimics an RbacGroupRoles link for OutputGroupRole.from_db."""
    link = MagicMock()
    link.id = uuid4()
    link.name = None
    link.details = None
    link.group = group or fake_group()
    link.role = role or fake_role()
    return link


def _fake_zone(id=None, name="zone"):
    z = MagicMock()
    z.id = id or uuid4()
    z.name = name
    z.details = {}
    return z


def _fake_core_channel(id=None, name="channel", zone_id=None):
    c = MagicMock()
    c.id = id or uuid4()
    c.name = name
    c.details = {}
    c.zone_id = zone_id or uuid4()
    zone = MagicMock()
    zone.id = c.zone_id
    zone.name = "zone"
    zone.details = {}
    c.zone = zone
    return c


def fake_zone_policy_mock(id=None, name="policy-name", **kwargs):
    """Build a MagicMock that mimics a ZonePolicy with loaded access relationship."""
    policy = MagicMock()
    policy.id = id or uuid4()
    policy.name = name
    policy.subject_id = kwargs.get("subject_id", uuid4())
    policy.subject = MagicMock()
    policy.subject.id = policy.subject_id
    policy.subject.type = kwargs.get("subject_type", "user")
    policy.subject.user_id = None
    policy.subject.group_id = None
    policy.subject.name = kwargs.get("subject_name", "subject-name")
    policy.subject.details = {}
    policy.is_delegation = kwargs.get("is_delegation", False)
    rid = kwargs.get("role_id", uuid4())
    profile = MagicMock()
    profile.id = uuid4()
    profile.name = "profile-name"
    profile.role_id = rid
    profile.role = fake_role(id=rid, name=kwargs.get("role_name", "role"))
    profile.description = None
    zid = kwargs.get("zone_id", uuid4())
    profile.zone_id = zid
    profile.zone = _fake_zone(id=zid, name=kwargs.get("zone_name", "zone"))
    policy.access_profile = profile
    return policy


def fake_channel_policy_mock(id=None, name="policy-name", **kwargs):
    """Build a MagicMock that mimics a ChannelPolicy with loaded access relationship."""
    policy = MagicMock()
    policy.id = id or uuid4()
    policy.name = name
    policy.subject_id = kwargs.get("subject_id", uuid4())
    policy.subject = MagicMock()
    policy.subject.id = policy.subject_id
    policy.subject.type = kwargs.get("subject_type", "user")
    policy.subject.user_id = None
    policy.subject.group_id = None
    policy.subject.name = kwargs.get("subject_name", "subject-name")
    policy.subject.details = {}
    policy.is_delegation = kwargs.get("is_delegation", False)
    rid = kwargs.get("role_id", uuid4())
    profile = MagicMock()
    profile.id = uuid4()
    profile.name = "profile-name"
    profile.role_id = rid
    profile.role = fake_role(id=rid, name=kwargs.get("role_name", "role"))
    profile.description = None
    chid = kwargs.get("channel_id", uuid4())
    profile.channel_id = chid
    profile.channel = _fake_core_channel(id=chid, name=kwargs.get("channel_name", "channel"))
    policy.access_profile = profile
    return policy


def _fake_core_row(id=None, name="row", channel_id=None):
    r = MagicMock()
    r.id = id or uuid4()
    r.name = name
    r.channel_id = channel_id
    r.details = {}
    ch_id = channel_id or uuid4()
    r.channel = OutputCoreChannel(id=ch_id, name="channel", details={})
    return r


def _fake_row_policy_mock(id=None, name="policy-name", **kwargs):
    """Build a MagicMock that mimics a RowPolicy with loaded access relationship."""
    policy = MagicMock()
    policy.id = id or uuid4()
    policy.name = name
    policy.subject_id = kwargs.get("subject_id", uuid4())
    policy.subject = MagicMock()
    policy.subject.id = policy.subject_id
    policy.subject.type = kwargs.get("subject_type", "user")
    policy.subject.user_id = None
    policy.subject.group_id = None
    policy.subject.name = kwargs.get("subject_name", "subject-name")
    policy.subject.details = {}
    policy.is_delegation = kwargs.get("is_delegation", False)
    rid = kwargs.get("role_id", uuid4())
    profile = MagicMock()
    profile.id = uuid4()
    profile.name = "profile-name"
    profile.role_id = rid
    profile.role = fake_role(id=rid, name=kwargs.get("role_name", "role"))
    profile.description = None
    row_id = kwargs.get("row_id", uuid4())
    profile.row_id = row_id
    profile.row = _fake_core_row(id=row_id, name=kwargs.get("row_name", "row"), channel_id=kwargs.get("channel_id"))
    policy.access_profile = profile
    return policy
