# tests/unit/services/test_rbac_service.py
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from kronicle.errors.error_types import BadRequestError, ConflictError, NotFoundError, UnauthorizedError
from kronicle.schemas.core.input_ressource_schema import InputCoreChannel, InputRow, InputZonePatch
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.permissions.permission import PermAction, Permission, PermTarget
from kronicle.schemas.rbac.input_policy_schemas import (
    InputChannelAccessProfile,
    InputRowAccessProfile,
    InputZoneAccessProfile,
)
from kronicle.schemas.rbac.input_role_schemas import InputRole
from kronicle.schemas.rbac.input_subject_schemas import InputSubject
from kronicle.schemas.rbac.input_user_schemas import InputUserLogin
from kronicle.schemas.rbac.safe_group_schemas import OutputGroup
from kronicle.schemas.rbac.safe_policy_schemas import (
    OutputChannelAccessProfile,
    OutputChannelPolicy,
    OutputRowAccessProfile,
    OutputRowPolicy,
    OutputZoneAccessProfile,
    OutputZonePolicy,
)
from kronicle.schemas.rbac.safe_role_schemas import OutputRole
from kronicle.schemas.rbac.safe_user_schemas import OutputUser, ProcessedUser
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


def _fake_user(id=None, name="usr", email="u@k.app"):
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


def _fake_role(id=None, name="role"):
    r = MagicMock()
    r.id = id or uuid4()
    r.name = name
    r.description = ""
    r.permissions = []
    r.restrictions = []
    r.details = {}
    r.snapshot = {"id": str(r.id), "name": name, "permissions": [], "restrictions": [], "is_global": False}
    return r


def _fake_group(id=None, name="grp"):
    g = MagicMock()
    g.id = id or uuid4()
    g.name = name
    g.details = {}
    g.snapshot = {"id": str(g.id), "name": name}
    return g


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


def _fake_zone_policy_mock(id=None, name="policy-name", **kwargs):
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
    profile.role = _fake_role(id=rid, name=kwargs.get("role_name", "role"))
    profile.description = None
    zid = kwargs.get("zone_id", uuid4())
    profile.zone_id = zid
    profile.zone = _fake_zone(id=zid, name=kwargs.get("zone_name", "zone"))
    policy.access_profile = profile
    return policy


def _fake_channel_policy_mock(id=None, name="policy-name", **kwargs):
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
    profile.role = _fake_role(id=rid, name=kwargs.get("role_name", "role"))
    profile.description = None
    cid = kwargs.get("channel_id", uuid4())
    profile.channel_id = cid
    profile.channel = _fake_core_channel(id=cid, name=kwargs.get("channel_name", "channel"))
    policy.access_profile = profile
    return policy


# ==================================================================================================
# User read methods
# ==================================================================================================


class TestUserRead:
    def test_fetch_user_for_auth(self, rbac_service):
        db_user = _fake_user()
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        login = MagicMock(spec=InputUserLogin)
        login.is_email = True
        login.login = "u@k.app"

        result = rbac_service.fetch_user_for_auth(login)
        assert result is db_user

    def test_fetch_user_for_auth_by_name(self, rbac_service):
        db_user = _fake_user(name="testuser")
        rbac_service._user_repo.get_by_name = MagicMock(return_value=db_user)
        login = MagicMock(spec=InputUserLogin)
        login.is_email = False
        login.login = "testuser"

        result = rbac_service.fetch_user_for_auth(login)
        assert result is db_user

    def test_fetch_user_for_auth_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_email = MagicMock(return_value=None)
        login = MagicMock(spec=InputUserLogin)
        login.is_email = True
        login.login = "noone@k.app"

        with pytest.raises(NotFoundError, match="User not found"):
            rbac_service.fetch_user_for_auth(login)

    def test_fetch_user_info(self, rbac_service):
        db_user = _fake_user()
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        login = MagicMock(spec=InputUserLogin)
        login.is_email = True
        login.login = "u@k.app"

        result = rbac_service.fetch_user_info(login)
        assert isinstance(result, OutputUser)
        assert result.email == db_user.email

    def test_fetch_user_by_email(self, rbac_service):
        db_user = _fake_user(email="found@k.app")
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)

        result = rbac_service.fetch_user_by_email("found@k.app")
        assert isinstance(result, OutputUser)
        assert result.email == "found@k.app"

    def test_fetch_user_by_email_none(self, rbac_service):
        rbac_service._user_repo.get_by_email = MagicMock(return_value=None)
        assert rbac_service.fetch_user_by_email("noone@k.app") is None

    def test_fetch_user_by_name(self, rbac_service):
        db_user = _fake_user(name="bob")
        rbac_service._user_repo.get_by_name = MagicMock(return_value=db_user)

        result = rbac_service.fetch_user_by_name("bob")
        assert isinstance(result, OutputUser)
        assert result.name == "bob"

    def test_fetch_user_by_name_none(self, rbac_service):
        rbac_service._user_repo.get_by_name = MagicMock(return_value=None)
        assert rbac_service.fetch_user_by_name("noone") is None

    def test_fetch_user_by_id(self, rbac_service):
        uid = uuid4()
        db_user = _fake_user(id=uid)
        rbac_service._user_repo.get_by_id = MagicMock(return_value=db_user)

        result = rbac_service.fetch_user_by_id(uid)
        assert isinstance(result, OutputUser)
        assert result.id == uid

    def test_fetch_user_by_id_none(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.fetch_user_by_id(uuid4()) is None

    def test_fetch_user_by_external_id(self, rbac_service):
        db_user = _fake_user()
        db_user.external_id = "ext-123"
        rbac_service._user_repo.get_by_external_id = MagicMock(return_value=db_user)

        result = rbac_service.fetch_user_by_external_id("ext-123")
        assert isinstance(result, OutputUser)

    def test_fetch_user_by_external_id_none(self, rbac_service):
        rbac_service._user_repo.get_by_external_id = MagicMock(return_value=None)
        assert rbac_service.fetch_user_by_external_id("ext-x") is None

    def test_list_users(self, rbac_service):
        rbac_service._user_repo.fetch_all = MagicMock(return_value=[_fake_user(), _fake_user()])
        result = rbac_service.list_users()
        assert len(result) == 2
        assert all(isinstance(u, OutputUser) for u in result)


# ==================================================================================================
# User write methods
# ==================================================================================================


class TestUserWrite:
    def test_create_user_no_password_hash(self, rbac_service):
        user = ProcessedUser(email="nopass@k.app", name="NoPass")
        with pytest.raises(BadRequestError, match="password"):
            rbac_service.create_user(user)

    def test_create_user_success(self, rbac_service):
        user = ProcessedUser(email="new@k.app", name="NewUser", password_hash="h")
        db_user = _fake_user(id=uuid4(), name="New", email="new@k.app")
        rbac_service._user_repo.get_by_email = MagicMock(return_value=None)
        rbac_service._user_repo.create_user = MagicMock(return_value=db_user)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)

        out = rbac_service.create_user(user)
        assert isinstance(out, OutputUser)
        assert out.email == user.email

    def test_create_user_already_exists(self, rbac_service):
        user = ProcessedUser(email="dup@k.app", name="DupUser", password_hash="h")
        rbac_service._user_repo.get_by_email = MagicMock(return_value=_fake_user())
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        with pytest.raises(UnauthorizedError, match="Email already in use"):
            rbac_service.create_user(user)

    def test_patch_user(self, rbac_service):
        db_user = _fake_user(email="old@k.app", name="old")
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        user = ProcessedUser(email="old@k.app", name="newname", password_hash="h")

        out = rbac_service.patch_user(user)
        assert out.name == "newname"
        assert db_user.name == "newname"

    def test_patch_user_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_email = MagicMock(return_value=None)
        user = ProcessedUser(email="noone@k.app", name="x_user", password_hash="h")
        with pytest.raises(UnauthorizedError, match="doesn't exists"):
            rbac_service.patch_user(user)

    def test_patch_user_integrity_error(self, rbac_service):
        from sqlalchemy.exc import IntegrityError

        db_user = _fake_user(email="u@k.app", name="u")
        db_user.full_name = None
        db_user.external_id = None
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        db.commit.side_effect = IntegrityError("stmt", {}, BaseException())

        user = ProcessedUser(email="u@k.app", name="newname", password_hash="h")
        with pytest.raises(UnauthorizedError, match="existing values"):
            rbac_service.patch_user(user)

    def test_patch_user_no_changes(self, rbac_service):
        db_user = _fake_user(email="u@k.app", name="u")
        db_user.full_name = None
        db_user.external_id = None
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        user = ProcessedUser(email="u@k.app", name="uname", password_hash="h")

        out = rbac_service.patch_user(user)
        assert out.name == "uname"

    def test_patch_user_by_id(self, rbac_service):
        uid = uuid4()
        db_user = _fake_user(id=uid, name="old")
        rbac_service._user_repo.get_by_id = MagicMock(return_value=db_user)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)

        out = rbac_service.patch_user_by_id(uid, name="newname", full_name="Full", orcid="ext-1")
        assert out.name == "newname"
        assert db_user.name == "newname"
        assert db_user.full_name == "Full"
        assert db_user.external_id == "ext-1"

    def test_patch_user_by_id_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_user_by_id(uuid4(), name="x")

    def test_patch_user_by_id_integrity_error(self, rbac_service):
        from sqlalchemy.exc import IntegrityError

        uid = uuid4()
        db_user = _fake_user(id=uid, name="old")
        rbac_service._user_repo.get_by_id = MagicMock(return_value=db_user)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        db.commit.side_effect = IntegrityError("stmt", {}, BaseException())

        with pytest.raises(BadRequestError, match="existing values"):
            rbac_service.patch_user_by_id(uid, name="newname")

    def test_patch_user_by_id_no_changes(self, rbac_service):
        uid = uuid4()
        db_user = _fake_user(id=uid, name="u")
        rbac_service._user_repo.get_by_id = MagicMock(return_value=db_user)

        out = rbac_service.patch_user_by_id(uid)
        assert out.name == "u"

    def test_update_password_hash(self, rbac_service):
        rbac_service._user_repo.update_password_hash = MagicMock()
        rbac_service.update_password_hash(uuid4(), "new_hash")
        rbac_service._user_repo.update_password_hash.assert_called_once()

    def test_deactivate_user(self, rbac_service):
        db_user = _fake_user(name="active")
        db_user.is_active = True
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        user = ProcessedUser(email="active@k.app", name="active", password_hash="h")

        out = rbac_service.deactivate_user(user)
        assert out is not None
        assert db_user.is_active is False

    def test_deactivate_user_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_email = MagicMock(return_value=None)
        user = ProcessedUser(email="noone@k.app", name="x_user", password_hash="h")
        with pytest.raises(UnauthorizedError):
            rbac_service.deactivate_user(user)

    def test_deactivate_user_by_id(self, rbac_service):
        uid = uuid4()
        db_user = _fake_user(id=uid, name="active")
        db_user.is_active = True
        rbac_service._user_repo.get_by_id = MagicMock(return_value=db_user)

        out = rbac_service.deactivate_user_by_id(uid)
        assert out is not None
        assert db_user.is_active is False

    def test_deactivate_user_by_id_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(UnauthorizedError):
            rbac_service.deactivate_user_by_id(uuid4())

    def test_remove_user(self, rbac_service):
        db_user = _fake_user(name="del")
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        rbac_service._user_repo.delete_user = MagicMock(return_value=db_user)
        user = ProcessedUser(email="del@k.app", name="deluser", password_hash="h")

        out = rbac_service.remove_user(user)
        assert out is not None

    def test_remove_user_by_id(self, rbac_service):
        uid = uuid4()
        db_user = _fake_user(id=uid, name="del")
        rbac_service._user_repo.get_by_id = MagicMock(return_value=db_user)
        rbac_service._user_repo.delete_user = MagicMock(return_value=db_user)

        out = rbac_service.remove_user_by_id(uid)
        assert out is not None


# ==================================================================================================
# Group methods
# ==================================================================================================


class TestGroups:
    def test_get_user_groups(self, rbac_service):
        gid = uuid4()
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={gid})
        result = rbac_service.get_user_groups(uuid4())
        assert result == [gid]

    @patch("kronicle.services.rbac_service.RbacGroup")
    def test_create_group(self, mock_rbac_group, rbac_service):
        mock_group = MagicMock()
        mock_group.id = uuid4()
        mock_group.name = "test-group"
        mock_group.details = {"k": "v"}
        mock_rbac_group.return_value = mock_group
        rbac_service._user_repo.get_by_name = MagicMock(return_value=None)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        out = rbac_service.create_group("test-group", details={"k": "v"})
        assert isinstance(out, OutputGroup)
        assert out.name == "test-group"

    def test_create_group_duplicate(self, rbac_service):
        rbac_service._user_repo.get_by_name = MagicMock(return_value=None)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=_fake_group(name="dup"))
        with pytest.raises(BadRequestError, match="already exists"):
            rbac_service.create_group("dup")

    def test_get_groups(self, rbac_service):
        rbac_service._group_repo.fetch_all = MagicMock(return_value=[_fake_group(), _fake_group()])
        result = rbac_service.get_groups()
        assert len(result) == 2

    def test_get_group_by_id(self, rbac_service):
        gid = uuid4()
        rbac_service._group_repo.get_by_id = MagicMock(return_value=_fake_group(id=gid))
        result = rbac_service.get_group_by_id(gid)
        assert isinstance(result, OutputGroup)
        assert result.id == gid

    def test_get_group_by_id_none(self, rbac_service):
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.get_group_by_id(uuid4()) is None

    def test_get_group_by_name(self, rbac_service):
        rbac_service._group_repo.get_by_name = MagicMock(return_value=_fake_group(name="found"))
        result = rbac_service.get_group_by_name("found")
        assert isinstance(result, OutputGroup)
        assert result.name == "found"

    def test_get_group_by_name_none(self, rbac_service):
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        assert rbac_service.get_group_by_name("noone") is None

    def test_get_users_from_group(self, rbac_service):
        uid = uuid4()
        rbac_service._user_groups_repo.get_user_ids_for_group = MagicMock(return_value={uid})
        rbac_service._user_repo.get_by_id = MagicMock(return_value=_fake_user(id=uid))
        result = rbac_service.get_users_from_group(group_id=uuid4())
        assert len(result) == 1
        assert result[0].id == uid

    def test_get_users_from_group_skips_missing(self, rbac_service):
        rbac_service._user_groups_repo.get_user_ids_for_group = MagicMock(return_value={uuid4()})
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.get_users_from_group(group_id=uuid4()) == []

    def test_patch_group(self, rbac_service):
        gid = uuid4()
        grp = _fake_group(id=gid, name="old")
        rbac_service._group_repo.get_by_id = MagicMock(return_value=grp)

        out = rbac_service.patch_group(gid, name="new", details={"k": "v"})
        assert out.name == "new"
        assert grp.name == "new"
        assert grp.details == {"k": "v"}

    def test_patch_group_not_found(self, rbac_service):
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_group(uuid4(), name="x")

    def test_delete_group_force(self, rbac_service):
        gid = uuid4()
        grp = _fake_group(id=gid, name="del")
        rbac_service._group_repo.get_by_id = MagicMock(return_value=grp)
        out = rbac_service.delete_group(gid, force=True)
        assert isinstance(out, OutputGroup)

    def test_delete_group_not_found(self, rbac_service):
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.delete_group(uuid4())

    def test_delete_group_with_users(self, rbac_service):
        gid = uuid4()
        grp = _fake_group(id=gid, name="del")
        rbac_service._group_repo.get_by_id = MagicMock(return_value=grp)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        db.execute.return_value.scalars.return_value.all.return_value = [_fake_user()]

        with pytest.raises(ConflictError, match="cannot be deleted"):
            rbac_service.delete_group(gid, force=False)

    def test_add_user_to_group(self, rbac_service):
        uid, gid = uuid4(), uuid4()
        rbac_service._user_repo.get_by_id = MagicMock(return_value=_fake_user(id=uid))
        rbac_service._group_repo.get_by_id = MagicMock(return_value=_fake_group(id=gid))
        rbac_service._user_groups_repo.add_user_to_group = MagicMock()

        rbac_service.add_user_to_group(uid, gid)
        rbac_service._user_groups_repo.add_user_to_group.assert_called_once()

    def test_add_user_to_group_user_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="User"):
            rbac_service.add_user_to_group(uuid4(), uuid4())

    def test_add_user_to_group_group_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=_fake_user())
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Group"):
            rbac_service.add_user_to_group(uuid4(), uuid4())

    def test_remove_user_from_group(self, rbac_service):
        uid, gid = uuid4(), uuid4()
        rbac_service._user_repo.get_by_id = MagicMock(return_value=_fake_user(id=uid))
        rbac_service._group_repo.get_by_id = MagicMock(return_value=_fake_group(id=gid))
        rbac_service._user_groups_repo.remove_user_from_group = MagicMock()

        rbac_service.remove_user_from_group(uid, gid)
        rbac_service._user_groups_repo.remove_user_from_group.assert_called_once()

    def test_remove_user_from_group_user_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="User"):
            rbac_service.remove_user_from_group(uuid4(), uuid4())


# ==================================================================================================
# Role methods
# ==================================================================================================


class TestRoles:
    @patch("kronicle.services.rbac_service.RbacRole")
    def test_create_role(self, mock_rbac_role, rbac_service):
        mock_role = MagicMock()
        mock_role.id = uuid4()
        mock_role.name = "test-role"
        mock_role.description = "desc"
        mock_role.permissions = ["zone:read"]
        mock_role.restrictions = []
        mock_role.details = {"k": "v"}
        mock_rbac_role.return_value = mock_role
        rbac_service._role_repo.get_by_name = MagicMock(return_value=None)
        out = rbac_service.create_role("test-role", description="desc", permissions=["zone:read"], details={"k": "v"})
        assert isinstance(out, OutputRole)
        assert out.name == "test-role"
        assert out.permissions == ["zone:read"]

    def test_create_role_duplicate(self, rbac_service):
        rbac_service._role_repo.get_by_name = MagicMock(return_value=_fake_role(name="dup"))
        with pytest.raises(BadRequestError, match="already exists"):
            rbac_service.create_role("dup")

    def test_get_roles(self, rbac_service):
        rbac_service._role_repo.fetch_all = MagicMock(return_value=[_fake_role(), _fake_role()])
        result = rbac_service.get_roles()
        assert len(result) == 2
        assert all(isinstance(r, OutputRole) for r in result)

    def test_get_role(self, rbac_service):
        rid = uuid4()
        rbac_service._role_repo.get_by_id = MagicMock(return_value=_fake_role(id=rid))
        result = rbac_service.get_role(rid)
        assert isinstance(result, OutputRole)
        assert result.id == rid

    def test_get_role_none(self, rbac_service):
        rbac_service._role_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.get_role(uuid4()) is None

    def test_get_role_by_name(self, rbac_service):
        rbac_service._role_repo.get_by_name = MagicMock(return_value=_fake_role(name="found"))
        result = rbac_service.get_role_by_name("found")
        assert isinstance(result, OutputRole)
        assert result.name == "found"

    def test_get_role_by_name_none(self, rbac_service):
        rbac_service._role_repo.get_by_name = MagicMock(return_value=None)
        assert rbac_service.get_role_by_name("noone") is None

    def test_patch_role(self, rbac_service):
        rid = uuid4()
        role = _fake_role(id=rid, name="old")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)

        out = rbac_service.patch_role(
            rid,
            name="new",
            description="new desc",
            permissions=["zone:write"],
            restrictions=["zone:delete"],
            details={"k": "v"},
        )
        assert out.name == "new"
        assert role.name == "new"
        assert role.description == "new desc"

    def test_patch_role_not_found(self, rbac_service):
        rbac_service._role_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_role(uuid4(), name="x")

    def test_patch_role_partial(self, rbac_service):
        rid = uuid4()
        role = _fake_role(id=rid, name="old")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)

        out = rbac_service.patch_role(rid, name="partial")
        assert out.name == "partial"

    def test_delete_role_force(self, rbac_service):
        rid = uuid4()
        role = _fake_role(id=rid, name="del")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        out = rbac_service.delete_role(rid, force=True)
        assert isinstance(out, OutputRole)

    def test_delete_role_not_found(self, rbac_service):
        rbac_service._role_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.delete_role(uuid4())

    def test_delete_role_with_users(self, rbac_service):
        rid = uuid4()
        role = _fake_role(id=rid, name="del")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        user = _fake_user()
        db.execute.side_effect = [
            MagicMock(scalars=lambda: MagicMock(all=lambda: [user])),
            MagicMock(scalars=lambda: MagicMock(all=lambda: [])),
        ]
        with pytest.raises(ConflictError, match="cannot be deleted"):
            rbac_service.delete_role(rid, force=False)

    def test_delete_role_with_groups(self, rbac_service):
        rid = uuid4()
        role = _fake_role(id=rid, name="del")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        db.execute.side_effect = [
            MagicMock(scalars=lambda: MagicMock(all=lambda: [])),
            MagicMock(scalars=lambda: MagicMock(all=lambda: [_fake_group()])),
        ]
        with pytest.raises(ConflictError, match="cannot be deleted"):
            rbac_service.delete_role(rid, force=False)


# ==================================================================================================
# User ↔ Role + Group ↔ Role assignments
# ==================================================================================================


class TestAssignments:
    def test_assign_role_to_user(self, rbac_service):
        rbac_service.assign_role_to_user(uuid4(), uuid4())
        assert rbac_service._db.transaction.return_value.__enter__.return_value.execute.called

    def test_remove_role_from_user(self, rbac_service):
        rbac_service.remove_role_from_user(uuid4(), uuid4())
        assert rbac_service._db.transaction.return_value.__enter__.return_value.execute.called

    def test_assign_role_to_group(self, rbac_service):
        rbac_service.assign_role_to_group(uuid4(), uuid4())
        assert rbac_service._db.transaction.return_value.__enter__.return_value.execute.called

    def test_remove_role_from_group(self, rbac_service):
        rbac_service.remove_role_from_group(uuid4(), uuid4())
        assert rbac_service._db.transaction.return_value.__enter__.return_value.execute.called


# ==================================================================================================
# Permissions
# ==================================================================================================


class TestPermissions:
    def test_user_has_permission_direct(self, rbac_service):
        uid = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.return_value = (uuid4(),)

        result = rbac_service.user_has_permission(uid, "zone:read")
        assert result is True

    def test_user_has_permission_via_group(self, rbac_service):
        uid, gid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value

        # Direct check: returns something that has .first() -> None
        mock_direct_res = MagicMock()
        mock_direct_res.first.return_value = None

        # Group check: returns something that has .first() -> (role_id,)
        mock_group_res = MagicMock()
        mock_group_res.first.return_value = (uuid4(),)

        db.execute.side_effect = [mock_direct_res, mock_group_res]
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={gid})

        result = rbac_service.user_has_permission(uid, "zone:read")
        assert result is True

    def test_user_has_permission_via_policy_direct(self, rbac_service):
        uid = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value

        # 1. Direct: None
        mock_direct_res = MagicMock()
        mock_direct_res.first.return_value = None

        # 2. Group check: None
        mock_group_res = MagicMock()
        mock_group_res.first.return_value = None

        # 3. Policy check: Found
        mock_policy_res = MagicMock()
        mock_policy_res.first.return_value = (uuid4(),)

        db.execute.side_effect = [mock_direct_res, mock_group_res, mock_policy_res]
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value=set())

        result = rbac_service.user_has_permission(uid, "zone:read")
        assert result is True

    def test_user_has_permission_anonymous(self, rbac_service):
        rbac_service._group_repo.get_by_name = MagicMock(return_value=_fake_group(name="anonymous"))
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.return_value = (uuid4(),)

        result = rbac_service.user_has_permission(None, "zone:read")
        assert result is True

    def test_user_has_permission_anonymous_no_group(self, rbac_service):
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        result = rbac_service.user_has_permission(None, "zone:read")
        assert result is False

    def test_user_has_permission_denied(self, rbac_service):
        uid = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.return_value = None
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value=set())

        result = rbac_service.user_has_permission(uid, "zone:read")
        assert result is False


# ==================================================================================================


class TestSubject:
    def test_ensure_subject_user(self, rbac_service):
        uid = uuid4()
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        subject = MagicMock()
        subject.type = "user"
        subject.name = "usr"
        rbac_service._subject_repo.ensure_from_user = MagicMock(return_value=subject)
        rbac_service._user_repo.get_by_id = MagicMock(return_value=_fake_user(id=uid))

        result = rbac_service._ensure_subject(db, InputSubject(id=uid, type="user", user_id=uid))

        assert result.type == "user"
        assert result.name == "usr"

    def test_ensure_subject_group(self, rbac_service):
        gid = uuid4()
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        subject = MagicMock()
        subject.type = "group"
        subject.name = "grp"
        rbac_service._subject_repo.ensure_from_group = MagicMock(return_value=subject)
        rbac_service._group_repo.get_by_id = MagicMock(return_value=_fake_group(id=gid))

        result = rbac_service._ensure_subject(db, InputSubject(id=gid, type="group", group_id=gid))

        assert result.type == "group"
        assert result.name == "grp"

    def test_ensure_subject_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)

        with pytest.raises(NotFoundError, match="User|Group"):
            rbac_service._ensure_subject(db, InputSubject(id=uuid4(), type="user", user_id=uuid4()))

    def test_ensure_subject_by_id_inactive_user(self, rbac_service):
        uid = uuid4()
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        user = _fake_user(id=uid)
        user.is_active = False
        rbac_service._user_repo.get_by_id = MagicMock(return_value=user)

        with pytest.raises(UnauthorizedError, match="inactive"):
            rbac_service._ensure_subject_by_id(db, InputSubject(id=uid, type="user", user_id=uid))

    def test_ensure_subject_by_name_user_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._user_repo.get_by_name = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="User"):
            rbac_service._ensure_subject_by_name(db, "user", "missing")

    def test_ensure_subject_by_name_group_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Group"):
            rbac_service._ensure_subject_by_name(db, "group", "missing")

    def test_ensure_subject_id_missing(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        with pytest.raises(BadRequestError, match="Either id or name"):
            rbac_service._ensure_subject(db, InputSubject(id=None, name=None, type="user"))


# ==================================================================================================
# Access profiles
# ==================================================================================================


class TestZoneAccessProfileCRUD:
    def _profile_mock(self, **kwargs):
        profile = MagicMock()
        profile.id = kwargs.get("id", uuid4())
        profile.name = kwargs.get("name", "zone-profile")
        profile.description = kwargs.get("description", None)
        rid = kwargs.get("role_id", uuid4())
        profile.role_id = rid
        profile.role = _fake_role(id=rid, name=kwargs.get("role_name", "r"))
        zid = kwargs.get("zone_id", uuid4())
        profile.zone_id = zid
        profile.zone = _fake_zone(id=zid, name=kwargs.get("zone_name", "z"))
        return profile

    def test_create(self, rbac_service):
        rid, zid = uuid4(), uuid4()
        profile = self._profile_mock(role_id=rid, zone_id=zid, description="desc")
        rbac_service._ensure_zone_access_profile = MagicMock(return_value=profile)

        out = rbac_service.create_zone_access_profile(
            profile_in=InputZoneAccessProfile(role=InputRole(id=rid), zone=InputZonePatch(id=zid), description="desc")
        )
        assert isinstance(out, OutputZoneAccessProfile)

    def test_list(self, rbac_service):
        profile = self._profile_mock()
        rbac_service._zone_access_profile_repo.fetch_all = MagicMock(return_value=[profile])

        result = rbac_service.list_zone_access_profiles()
        assert len(result) == 1
        assert isinstance(result[0], OutputZoneAccessProfile)

    def test_get(self, rbac_service):
        pid = uuid4()
        profile = self._profile_mock(id=pid)
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=profile)

        result = rbac_service.get_zone_access_profile(pid)
        assert isinstance(result, OutputZoneAccessProfile)

    def test_get_none(self, rbac_service):
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.get_zone_access_profile(uuid4()) is None

    def test_delete(self, rbac_service):
        pid = uuid4()
        profile = self._profile_mock(id=pid)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=profile)

        result = rbac_service.delete_zone_access_profile(pid)
        db.delete.assert_called_once_with(profile)
        assert isinstance(result, OutputZoneAccessProfile)

    def test_delete_not_found(self, rbac_service):
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.delete_zone_access_profile(uuid4())


class TestChannelAccessProfileCRUD:
    def _profile_mock(self, **kwargs):
        profile = MagicMock()
        profile.id = kwargs.get("id", uuid4())
        profile.name = kwargs.get("name", "channel-profile")
        profile.description = kwargs.get("description", None)
        rid = kwargs.get("role_id", uuid4())
        profile.role_id = rid
        profile.role = _fake_role(id=rid, name=kwargs.get("role_name", "r"))
        cid = kwargs.get("channel_id", uuid4())
        profile.channel_id = cid
        profile.channel = _fake_core_channel(id=cid, name=kwargs.get("channel_name", "c"))
        return profile

    def test_create(self, rbac_service):
        rid, cid = uuid4(), uuid4()
        profile = self._profile_mock(role_id=rid, channel_id=cid, description="desc")
        rbac_service._ensure_channel_access_profile = MagicMock(return_value=profile)

        out = rbac_service.create_channel_access_profile(
            profile_in=InputChannelAccessProfile(
                role=InputRole(id=rid), channel=InputPayload(id=cid), description="desc"
            )
        )
        assert isinstance(out, OutputChannelAccessProfile)

    def test_list(self, rbac_service):
        profile = self._profile_mock()
        rbac_service._channel_access_profile_repo.fetch_all = MagicMock(return_value=[profile])

        result = rbac_service.list_channel_access_profiles()
        assert len(result) == 1
        assert isinstance(result[0], OutputChannelAccessProfile)

    def test_get(self, rbac_service):
        pid = uuid4()
        profile = self._profile_mock(id=pid)
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=profile)

        result = rbac_service.get_channel_access_profile(pid)
        assert isinstance(result, OutputChannelAccessProfile)

    def test_get_none(self, rbac_service):
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.get_channel_access_profile(uuid4()) is None

    def test_delete(self, rbac_service):
        pid = uuid4()
        profile = self._profile_mock(id=pid)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=profile)

        result = rbac_service.delete_channel_access_profile(pid)
        db.delete.assert_called_once_with(profile)
        assert isinstance(result, OutputChannelAccessProfile)

    def test_delete_not_found(self, rbac_service):
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.delete_channel_access_profile(uuid4())


# ==================================================================================================
# Policies
# ==================================================================================================


class TestZonePolicy:
    def test_create(self, rbac_service):
        sid, rid, zid = uuid4(), uuid4(), uuid4()
        role = _fake_role(id=rid, name="role")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)

        with patch.object(OutputZonePolicy, "from_db") as mock_from_db:
            expected = MagicMock(spec=OutputZonePolicy)
            expected.role = MagicMock()
            expected.role.name = "role"
            expected.role.id = rid
            expected.subject = MagicMock()
            expected.subject.id = sid
            expected.zone = MagicMock()
            expected.zone.id = zid
            mock_from_db.return_value = expected

            result = rbac_service.create_zone_policy(
                subject=InputSubject(id=sid, type="user", user_id=sid),
                access_profile=InputZoneAccessProfile(
                    role=InputRole(id=rid),
                    zone=InputZonePatch(id=zid),
                ),
            )

            assert result.role.name == "role"
            assert result.subject.id == sid
            assert result.role.id == rid
            assert result.zone.id == zid

    def test_create_role_not_found(self, rbac_service):
        rbac_service._role_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Role"):
            rbac_service.create_zone_policy(
                subject=InputSubject(id=uuid4(), type="user", user_id=uuid4()),
                access_profile=InputZoneAccessProfile(
                    role=InputRole(id=uuid4()),
                    zone=InputZonePatch(id=uuid4()),
                ),
            )

    def test_list(self, rbac_service):
        zid = uuid4()
        policy = _fake_zone_policy_mock(zone_id=zid)

        rbac_service._zone_policy_repo.get_policies_for_zone = MagicMock(return_value=[policy])

        result = rbac_service.list_policies_for_zone(zid)
        assert len(result) == 1
        assert isinstance(result[0], OutputZonePolicy)
        assert result[0].access_profile.zone.id == zid

    def test_delete(self, rbac_service):
        pid = uuid4()
        policy = _fake_zone_policy_mock(id=pid)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._zone_policy_repo.get_by_id = MagicMock(return_value=policy)

        result = rbac_service.delete_zone_policy(pid)
        db.delete.assert_called_once_with(policy)
        assert isinstance(result, OutputZonePolicy)

    def test_delete_not_found(self, rbac_service):
        rbac_service._zone_policy_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.delete_zone_policy(uuid4())


class TestChannelPolicy:
    def test_create(self, rbac_service):
        sid, rid, cid = uuid4(), uuid4(), uuid4()
        role = _fake_role(id=rid, name="role")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)

        with patch.object(OutputChannelPolicy, "from_db") as mock_from_db:
            expected = MagicMock()
            expected.role = MagicMock()
            expected.role.name = "role"
            expected.role.id = rid
            expected.subject = MagicMock()
            expected.subject.id = sid
            expected.access_profile = MagicMock()
            expected.access_profile.role = MagicMock()
            expected.access_profile.role.id = rid
            expected.access_profile.channel = MagicMock()
            expected.access_profile.channel.id = cid
            mock_from_db.return_value = expected

            result = rbac_service.create_channel_policy(
                subject=InputSubject(id=sid, type="user", user_id=sid),
                access_profile=InputChannelAccessProfile(
                    role=InputRole(id=rid),
                    channel=InputPayload(id=cid),
                ),
            )

            assert result.role.name == "role"
            assert result.subject.id == sid
            assert result.access_profile.role.id == rid
            assert result.access_profile.channel.id == cid

    def test_create_role_not_found(self, rbac_service):
        rbac_service._role_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Role"):
            rbac_service.create_channel_policy(
                subject=InputSubject(id=uuid4(), type="user", user_id=uuid4()),
                access_profile=InputChannelAccessProfile(
                    role=InputRole(id=uuid4()),
                    channel=InputPayload(id=uuid4()),
                ),
            )

    def test_list(self, rbac_service):
        cid = uuid4()
        policy = _fake_channel_policy_mock(channel_id=cid)

        rbac_service._channel_policy_repo.get_policies_for_channel = MagicMock(return_value=[policy])

        result = rbac_service.list_policies_for_channel(cid)
        assert len(result) == 1
        assert isinstance(result[0], OutputChannelPolicy)
        assert result[0].access_profile.channel.id == cid

    def test_delete(self, rbac_service):
        pid = uuid4()
        policy = _fake_channel_policy_mock(id=pid)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._channel_policy_repo.get_by_id = MagicMock(return_value=policy)

        result = rbac_service.delete_channel_policy(pid)
        db.delete.assert_called_once_with(policy)
        assert isinstance(result, OutputChannelPolicy)

    def test_delete_not_found(self, rbac_service):
        rbac_service._channel_policy_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.delete_channel_policy(uuid4())


# ==================================================================================================
# Relationship checks
# ==================================================================================================


class TestUserHasRole:
    def test_direct(self, rbac_service):
        uid, rid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = (uid, rid)

        result = rbac_service.check_user_has_role(uid, rid)
        assert result == {"has_role": True, "direct": True}

    def test_not_direct_no_indirect(self, rbac_service):
        uid, rid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = None

        result = rbac_service.check_user_has_role(uid, rid, indirect=False)
        assert result == {"has_role": False, "direct": False}

    def test_via_group(self, rbac_service):
        uid, rid, gid = uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.side_effect = [None, (gid, rid)]
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={gid})

        result = rbac_service.check_user_has_role(uid, rid, indirect=True)
        assert result == {"has_role": True, "direct": False}

    def test_not_found_indirect(self, rbac_service):
        uid, rid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        query_mock = MagicMock()
        db.query.return_value = query_mock
        query_mock.filter_by.return_value.first.return_value = None
        query_mock.filter.return_value.first.return_value = None
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value=set())

        result = rbac_service.check_user_has_role(uid, rid, indirect=True)
        assert result == {"has_role": False, "direct": False}

    def test_via_group_with_ancestors(self, rbac_service):
        uid, rid, gid, aid = uuid4(), uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.side_effect = [None, (aid, rid)]
        db.query.return_value.filter.return_value.all.return_value = [(aid,)]
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={gid})

        result = rbac_service.check_user_has_role(uid, rid, indirect=True)
        assert result == {"has_role": True, "direct": False}


class TestGroupHasRole:
    def test_direct(self, rbac_service):
        gid, rid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = (gid, rid)

        result = rbac_service.check_group_has_role(gid, rid)
        assert result == {"has_role": True, "direct": True}

    def test_not_direct_no_indirect(self, rbac_service):
        gid, rid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = None

        result = rbac_service.check_group_has_role(gid, rid, indirect=False)
        assert result == {"has_role": False, "direct": False}

    def test_via_ancestor(self, rbac_service):
        gid, rid, aid = uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.side_effect = [None, (aid, rid)]
        db.query.return_value.filter.return_value.all.return_value = [(aid,)]

        result = rbac_service.check_group_has_role(gid, rid, indirect=True)
        assert result == {"has_role": True, "direct": False}


class TestListRoleSubjects:
    def test_direct(self, rbac_service):
        rid = uuid4()
        uid1, uid2, gid = uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter.return_value.all.side_effect = [
            [(uid1,), (uid2,)],
            [(gid,)],
        ]

        result = rbac_service.list_role_subjects(rid, indirect=False)
        assert result["users"] == [str(uid1), str(uid2)]
        assert result["groups"] == [str(gid)]

    def test_indirect(self, rbac_service):
        rid = uuid4()
        uid, gid, member_uid = uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter.return_value.all.side_effect = [
            [(uid,)],
            [(gid,)],
            [(gid,)],
            [],
        ]
        rbac_service._user_groups_repo.get_user_ids_for_group = MagicMock(return_value={member_uid})

        result = rbac_service.list_role_subjects(rid, indirect=True)
        assert str(uid) in result["users"]
        assert str(gid) in result["groups"]
        assert str(member_uid) in result["indirect_users"]


class TestUserInGroup:
    def test_direct(self, rbac_service):
        uid, gid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = (uid, gid)

        result = rbac_service.check_user_in_group(uid, gid)
        assert result == {"is_member": True, "direct": True}

    def test_not_direct_no_indirect(self, rbac_service):
        uid, gid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = None

        result = rbac_service.check_user_in_group(uid, gid, indirect=False)
        assert result == {"is_member": False, "direct": False}

    def test_via_descendant(self, rbac_service):
        uid, gid, did = uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = None
        db.query.return_value.filter.return_value.all.return_value = [(did,)]
        rbac_service._user_groups_repo.get_user_ids_for_group = MagicMock(return_value={uid})

        result = rbac_service.check_user_in_group(uid, gid, indirect=True)
        assert result == {"is_member": True, "direct": False}

    def test_not_found_indirect(self, rbac_service):
        uid, gid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = None
        db.query.return_value.filter.return_value.all.return_value = []

        result = rbac_service.check_user_in_group(uid, gid, indirect=True)
        assert result == {"is_member": False, "direct": False}

    def test_via_descendant_no_membership(self, rbac_service):
        uid, gid, did = uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter_by.return_value.first.return_value = None
        db.query.return_value.filter.return_value.all.return_value = [(did,)]
        rbac_service._user_groups_repo.get_user_ids_for_group = MagicMock(return_value=set())

        result = rbac_service.check_user_in_group(uid, gid, indirect=True)
        assert result == {"is_member": False, "direct": False}


# ==================================================================================================
# Hierarchy helpers
# ==================================================================================================


class TestHierarchy:
    def test_get_group_ancestor_ids(self, rbac_service):
        gid, pid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter.return_value.all.return_value = [(pid,)]

        result = rbac_service._get_group_ancestor_ids(db, gid)
        assert pid in result

    def test_get_group_ancestor_ids_multi_level(self, rbac_service):
        gid, p1, p2 = uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter.return_value.all.side_effect = [[(p1,)], [(p2,)], []]

        result = rbac_service._get_group_ancestor_ids(db, gid)
        assert p1 in result and p2 in result

    def test_get_group_descendant_ids(self, rbac_service):
        gid, cid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.query.return_value.filter.return_value.all.return_value = [(cid,)]

        result = rbac_service._get_group_descendant_ids(db, gid)
        assert cid in result


# ==================================================================================================
# user_has_permission (from original test file)
# ==================================================================================================


class TestUserHasPermission:
    def test_direct_role(self, rbac_service):
        user_id = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.return_value = (uuid4(),)

        result = rbac_service.user_has_permission(user_id, "zone:read")
        assert result is True
        db.execute.assert_called_once()

    def test_group_role(self, rbac_service):
        user_id = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.side_effect = [None, (uuid4(),)]
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={uuid4()})

        result = rbac_service.user_has_permission(user_id, "zone:create")
        assert result is True
        assert db.execute.call_count == 2

    def test_no_match(self, rbac_service):
        user_id = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.return_value = None
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value=set())

        result = rbac_service.user_has_permission(user_id, "zone:read")
        assert result is False
        assert db.execute.call_count == 4

    def test_zone_policy_role(self, rbac_service):
        user_id = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.side_effect = [None, None, (uuid4(),)]
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value=set())

        result = rbac_service.user_has_permission(user_id, "group:read")
        assert result is True

    def test_empty_groups_no_match(self, rbac_service):
        user_id = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.side_effect = [None, None] + [None] * 12
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={uuid4()})

        result = rbac_service.user_has_permission(user_id, "zone:read")
        assert result is False

    def test_with_permission_object(self, rbac_service):
        user_id = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value
        db.execute.return_value.first.return_value = (uuid4(),)

        perm = Permission(PermTarget.ZONE, PermAction.READ)
        result = rbac_service.user_has_permission(user_id, perm)
        assert result is True
        db.execute.assert_called_once()


# ==================================================================================================
# Name reservation
# ==================================================================================================


class TestNameReservation:
    def test_reserved_name_user(self, rbac_service):
        user = ProcessedUser(email="admin@k.app", name="admin", password_hash="h")
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        with pytest.raises(BadRequestError, match="reserved"):
            rbac_service.create_user(user)

    def test_reserved_name_group(self, rbac_service):
        rbac_service._user_repo.get_by_name = MagicMock(return_value=None)
        with pytest.raises(BadRequestError, match="reserved"):
            rbac_service.create_group("admin")

    def test_user_name_clashes_with_group(self, rbac_service):
        rbac_service._group_repo.get_by_name = MagicMock(return_value=_fake_group(name="taken"))
        user = ProcessedUser(email="t@k.app", name="taken", password_hash="h")
        with pytest.raises(BadRequestError, match="group named"):
            rbac_service.create_user(user)

    def test_group_name_clashes_with_user(self, rbac_service):
        rbac_service._user_repo.get_by_name = MagicMock(return_value=_fake_user(name="taken"))
        with pytest.raises(BadRequestError, match="user named"):
            rbac_service.create_group("taken")


# ==================================================================================================
# Patch user extra fields
# ==================================================================================================


class TestPatchUserFields:
    def test_patch_user_full_name(self, rbac_service):
        db_user = _fake_user(email="u@k.app", name="u")
        db_user.full_name = None
        db_user.external_id = None
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        user = ProcessedUser(email="u@k.app", full_name="Full Name", password_hash="h")

        out = rbac_service.patch_user(user)
        assert out.full_name == "Full Name"
        assert db_user.full_name == "Full Name"

    def test_patch_user_external_id(self, rbac_service):
        db_user = _fake_user(email="u@k.app", name="u")
        db_user.full_name = None
        db_user.external_id = None
        rbac_service._user_repo.get_by_email = MagicMock(return_value=db_user)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        user = ProcessedUser(email="u@k.app", external_id="orcid-123", password_hash="h")

        out = rbac_service.patch_user(user)
        assert out.orcid == "orcid-123"
        assert db_user.external_id == "orcid-123"


# ==================================================================================================
# Delete user not found
# ==================================================================================================


class TestDeleteUserNotFound:
    def test_remove_user_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_email = MagicMock(return_value=None)
        user = ProcessedUser(email="noone@k.app", name="x_user", password_hash="h")
        with pytest.raises(UnauthorizedError, match="doesn't exists"):
            rbac_service.remove_user(user)

    def test_remove_user_by_id_not_found(self, rbac_service):
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(UnauthorizedError, match="doesn't exists"):
            rbac_service.remove_user_by_id(uuid4())


# ==================================================================================================
# _ensure_subject_by_id edge cases
# ==================================================================================================


class TestEnsureSubjectByIdEdges:
    def test_user_subject_missing_user_id(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        with pytest.raises(BadRequestError, match="user_id must be provided"):
            rbac_service._ensure_subject_by_id(db, InputSubject(id=uuid4(), type="user", user_id=None))

    def test_user_subject_user_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._user_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="User"):
            rbac_service._ensure_subject_by_id(db, InputSubject(id=uuid4(), type="user", user_id=uuid4()))

    def test_group_subject_missing_group_id(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        with pytest.raises(BadRequestError, match="group_id must be provided"):
            rbac_service._ensure_subject_by_id(db, InputSubject(id=uuid4(), type="group", group_id=None))

    def test_group_subject_group_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Group"):
            rbac_service._ensure_subject_by_id(db, InputSubject(id=uuid4(), type="group", group_id=uuid4()))


# ==================================================================================================
# _ensure_subject_by_name success paths + _ensure_subject name path
# ==================================================================================================


class TestEnsureSubjectByNameSuccess:
    def test_ensure_subject_by_name_user_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        user = _fake_user(name="alice")
        rbac_service._user_repo.get_by_name = MagicMock(return_value=user)
        subject = MagicMock()
        subject.name = "alice"
        rbac_service._subject_repo.ensure_from_user = MagicMock(return_value=subject)

        result = rbac_service._ensure_subject_by_name(db, "user", "alice")
        assert result.name == "alice"
        rbac_service._subject_repo.ensure_from_user.assert_called_once_with(db, user=user)

    def test_ensure_subject_by_name_group_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        group = _fake_group(name="team-a")
        rbac_service._group_repo.get_by_name = MagicMock(return_value=group)
        subject = MagicMock()
        subject.name = "team-a"
        rbac_service._subject_repo.ensure_from_group = MagicMock(return_value=subject)

        result = rbac_service._ensure_subject_by_name(db, "group", "team-a")
        assert result.name == "team-a"
        rbac_service._subject_repo.ensure_from_group.assert_called_once_with(db, group=group)

    def test_ensure_subject_name_path(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        group = _fake_group(name="mygroup")
        rbac_service._group_repo.get_by_name = MagicMock(return_value=group)
        subject = MagicMock()
        subject.name = "mygroup"
        rbac_service._subject_repo.ensure_from_group = MagicMock(return_value=subject)

        result = rbac_service._ensure_subject(db, InputSubject(id=None, name="mygroup", type="group"))
        assert result.name == "mygroup"


# ==================================================================================================
# _ensure_zone_access_profile internal logic
# ==================================================================================================


class TestEnsureZoneAccessProfile:
    def test_existing_by_id(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        pid = uuid4()
        existing = MagicMock()
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=existing)

        result = rbac_service._ensure_zone_access_profile(
            db, InputZoneAccessProfile(id=pid, role=InputRole(id=uuid4()), zone=InputZonePatch(id=uuid4()))
        )
        assert result is existing

    def test_existing_by_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        existing = MagicMock()
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._zone_access_profile_repo.get_by_name = MagicMock(return_value=existing)

        result = rbac_service._ensure_zone_access_profile(
            db, InputZoneAccessProfile(name="my-profile", role=InputRole(id=uuid4()), zone=InputZonePatch(id=uuid4()))
        )
        assert result is existing

    def test_create_new_generates_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rid, zid = uuid4(), uuid4()
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._zone_access_profile_repo.get_by_name = MagicMock(return_value=None)
        rbac_service._zone_access_profile_repo.get_by_role_and_zone = MagicMock(return_value=None)
        role = _fake_role(id=rid, name="reader")
        zone = _fake_zone(id=zid, name="my_zone")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=zone)
        new_profile = MagicMock()
        new_profile.description = None
        new_profile.details = None
        rbac_service._zone_access_profile_repo.create = MagicMock(return_value=new_profile)

        result = rbac_service._ensure_zone_access_profile(
            db, InputZoneAccessProfile(role=InputRole(id=rid), zone=InputZonePatch(id=zid))
        )
        rbac_service._zone_access_profile_repo.create.assert_called_once()
        assert result is new_profile

    def test_create_new_with_description_and_details(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rid, zid = uuid4(), uuid4()
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._zone_access_profile_repo.get_by_name = MagicMock(return_value=None)
        rbac_service._zone_access_profile_repo.get_by_role_and_zone = MagicMock(return_value=None)
        role = _fake_role(id=rid, name="r")
        zone = _fake_zone(id=zid, name="z")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=zone)
        new_profile = MagicMock()
        new_profile.description = None
        new_profile.details = None
        rbac_service._zone_access_profile_repo.create = MagicMock(return_value=new_profile)

        result = rbac_service._ensure_zone_access_profile(
            db,
            InputZoneAccessProfile(
                role=InputRole(id=rid),
                zone=InputZonePatch(id=zid),
                description="my desc",
                details={"k": "v"},
            ),
        )
        assert result.description == "my desc"
        assert result.details == {"k": "v"}

    def test_existing_by_role_and_zone(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rid, zid = uuid4(), uuid4()
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._zone_access_profile_repo.get_by_name = MagicMock(return_value=None)
        role = _fake_role(id=rid, name="r")
        zone = _fake_zone(id=zid, name="z")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=zone)
        existing = MagicMock()
        rbac_service._zone_access_profile_repo.get_by_role_and_zone = MagicMock(return_value=existing)

        result = rbac_service._ensure_zone_access_profile(
            db, InputZoneAccessProfile(role=InputRole(id=rid), zone=InputZonePatch(id=zid))
        )
        assert result is existing


# ==================================================================================================
# _ensure_channel_access_profile internal logic
# ==================================================================================================


class TestEnsureChannelAccessProfile:
    def test_existing_by_id(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        pid = uuid4()
        existing = MagicMock()
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=existing)

        result = rbac_service._ensure_channel_access_profile(
            db, InputChannelAccessProfile(id=pid, role=InputRole(id=uuid4()), channel=InputPayload(id=uuid4()))
        )
        assert result is existing

    def test_existing_by_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        existing = MagicMock()
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._channel_access_profile_repo.get_by_name = MagicMock(return_value=existing)

        result = rbac_service._ensure_channel_access_profile(
            db,
            InputChannelAccessProfile(name="my-profile", role=InputRole(id=uuid4()), channel=InputPayload(id=uuid4())),
        )
        assert result is existing

    def test_create_new_strips_channel_prefix(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rid, cid = uuid4(), uuid4()
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._channel_access_profile_repo.get_by_name = MagicMock(return_value=None)
        role = _fake_role(id=rid, name="reader")
        channel = _fake_core_channel(id=cid, name="channel_my_data")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=channel)
        new_profile = MagicMock()
        new_profile.description = None
        new_profile.details = None
        rbac_service._channel_access_profile_repo.create = MagicMock(return_value=new_profile)

        rbac_service._ensure_channel_access_profile(
            db, InputChannelAccessProfile(role=InputRole(id=rid), channel=InputPayload(id=cid))
        )
        call_kwargs = rbac_service._channel_access_profile_repo.create.call_args
        assert "my_data" in call_kwargs[1].get("name", call_kwargs[0][-1] if len(call_kwargs[0]) > 2 else "")

    def test_create_new_with_description_and_details(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rid, cid = uuid4(), uuid4()
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._channel_access_profile_repo.get_by_name = MagicMock(return_value=None)
        role = _fake_role(id=rid, name="r")
        channel = _fake_core_channel(id=cid, name="chan")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=channel)
        new_profile = MagicMock()
        new_profile.description = None
        new_profile.details = None
        rbac_service._channel_access_profile_repo.create = MagicMock(return_value=new_profile)

        result = rbac_service._ensure_channel_access_profile(
            db,
            InputChannelAccessProfile(
                role=InputRole(id=rid),
                channel=InputPayload(id=cid),
                description="desc",
                details={"k": "v"},
            ),
        )
        assert result.description == "desc"
        assert result.details == {"k": "v"}


# ==================================================================================================
# _ensure_row_access_profile
# ==================================================================================================


class TestEnsureRowAccessProfile:
    def test_existing_by_id(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        pid = uuid4()
        existing = MagicMock()
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=existing)

        row_id = uuid4()
        channel = _fake_core_channel()
        result = rbac_service._ensure_row_access_profile(
            db,
            InputRowAccessProfile(
                id=pid, role=InputRole(id=uuid4()), row=InputRow(id=row_id, channel=InputCoreChannel(id=channel.id))
            ),
        )
        assert result is existing

    def test_existing_by_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        existing = MagicMock()
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._row_access_profile_repo.get_by_name = MagicMock(return_value=existing)

        row_id = uuid4()
        channel = _fake_core_channel()
        result = rbac_service._ensure_row_access_profile(
            db,
            InputRowAccessProfile(
                name="my-row-profile",
                role=InputRole(id=uuid4()),
                row=InputRow(id=row_id, channel=InputCoreChannel(id=channel.id)),
            ),
        )
        assert result is existing

    def test_create_new(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rid = uuid4()
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._row_access_profile_repo.get_by_name = MagicMock(return_value=None)
        role = _fake_role(id=rid, name="reader")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        new_profile = MagicMock()
        new_profile.description = None
        new_profile.details = None
        rbac_service._row_access_profile_repo.create = MagicMock(return_value=new_profile)

        row_id = uuid4()
        channel = _fake_core_channel()
        result = rbac_service._ensure_row_access_profile(
            db,
            InputRowAccessProfile(
                role=InputRole(id=rid),
                row=InputRow(id=row_id, channel=InputCoreChannel(id=channel.id)),
                description="desc",
                details={"k": "v"},
            ),
        )
        assert result.description == "desc"
        assert result.details == {"k": "v"}

    def test_create_new_generates_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rid = uuid4()
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._row_access_profile_repo.get_by_name = MagicMock(return_value=None)
        role = _fake_role(id=rid, name="reader")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        new_profile = MagicMock()
        new_profile.description = None
        new_profile.details = None
        rbac_service._row_access_profile_repo.create = MagicMock(return_value=new_profile)

        row_id = uuid4()
        channel = _fake_core_channel()
        rbac_service._ensure_row_access_profile(
            db,
            InputRowAccessProfile(
                role=InputRole(id=rid),
                row=InputRow(id=row_id, channel=InputCoreChannel(id=channel.id)),
            ),
        )
        rbac_service._row_access_profile_repo.create.assert_called_once()


# ==================================================================================================
# _resolve_role / _resolve_zone / _resolve_channel
# ==================================================================================================


class TestResolvers:
    def test_resolve_role_by_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        role = _fake_role(name="reader")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._role_repo.get_by_name = MagicMock(return_value=role)

        result = rbac_service._resolve_role(db, InputRole(name="reader"))
        assert result.name == "reader"

    def test_resolve_role_by_name_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._role_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._role_repo.get_by_name = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Role"):
            rbac_service._resolve_role(db, InputRole(name="nonexistent"))

    def test_resolve_zone_by_id_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Zone"):
            rbac_service._resolve_zone(db, InputZonePatch(id=uuid4()))

    def test_resolve_zone_by_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        zone = _fake_zone(name="prod")
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._zone_repo.get_by_name = MagicMock(return_value=zone)

        result = rbac_service._resolve_zone(db, InputZonePatch(name="prod"))
        assert result.name == "prod"

    def test_resolve_zone_by_name_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._zone_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._zone_repo.get_by_name = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Zone"):
            rbac_service._resolve_zone(db, InputZonePatch(name="nope"))

    def test_resolve_zone_missing_id_and_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        with pytest.raises(BadRequestError, match="Either id or name"):
            rbac_service._resolve_zone(db, InputZonePatch(id=None, name=None))

    def test_resolve_role_missing_id_and_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        with pytest.raises(BadRequestError, match="Either id or name"):
            rbac_service._resolve_role(db, InputRole())

    def test_resolve_channel_missing_id_and_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        with pytest.raises(BadRequestError, match="Either id or name"):
            rbac_service._resolve_channel(db, InputPayload())

    def test_resolve_channel_by_id_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Channel"):
            rbac_service._resolve_channel(db, InputPayload(id=uuid4()))

    def test_resolve_channel_by_name(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        channel = _fake_core_channel(name="data-ch")
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._channel_repo.get_by_name = MagicMock(return_value=channel)

        result = rbac_service._resolve_channel(db, InputPayload(name="data-ch"))
        assert result.name == "data-ch"

    def test_resolve_channel_by_name_not_found(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._channel_repo.get_by_id = MagicMock(return_value=None)
        rbac_service._channel_repo.get_by_name = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Channel"):
            rbac_service._resolve_channel(db, InputPayload(name="nope"))


# ==================================================================================================
# patch access profiles
# ==================================================================================================


class TestPatchZoneAccessProfile:
    def test_patch_name_and_role(self, rbac_service):
        pid = uuid4()
        profile = MagicMock()
        profile.name = "old"
        profile.description = None
        profile.details = None
        rid = uuid4()
        role = _fake_role(id=rid, name="new-role")
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=profile)
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)

        with patch.object(OutputZoneAccessProfile, "from_db", return_value=MagicMock(spec=OutputZoneAccessProfile)):
            rbac_service.patch_zone_access_profile(pid, name="new-name", role=InputRole(id=rid))
        assert profile.name == "new-name"
        assert profile.role_id == rid

    def test_patch_description_and_details(self, rbac_service):
        pid = uuid4()
        profile = MagicMock()
        profile.name = "p"
        profile.description = "old"
        profile.details = {}
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=profile)

        with patch.object(OutputZoneAccessProfile, "from_db", return_value=MagicMock(spec=OutputZoneAccessProfile)):
            rbac_service.patch_zone_access_profile(pid, description="new desc", details={"k": "v"})
        assert profile.description == "new desc"
        assert profile.details == {"k": "v"}

    def test_patch_not_found(self, rbac_service):
        rbac_service._zone_access_profile_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_zone_access_profile(uuid4(), name="x")


class TestPatchChannelAccessProfile:
    def test_patch_name_and_role(self, rbac_service):
        pid = uuid4()
        profile = MagicMock()
        profile.name = "old"
        profile.description = None
        profile.details = None
        rid = uuid4()
        role = _fake_role(id=rid, name="new-role")
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=profile)
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)

        with patch.object(
            OutputChannelAccessProfile, "from_db", return_value=MagicMock(spec=OutputChannelAccessProfile)
        ):
            rbac_service.patch_channel_access_profile(pid, name="new-name", role=InputRole(id=rid))
        assert profile.name == "new-name"
        assert profile.role_id == rid

    def test_patch_description_and_details(self, rbac_service):
        pid = uuid4()
        profile = MagicMock()
        profile.name = "p"
        profile.description = "old"
        profile.details = {}
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=profile)

        with patch.object(
            OutputChannelAccessProfile, "from_db", return_value=MagicMock(spec=OutputChannelAccessProfile)
        ):
            rbac_service.patch_channel_access_profile(pid, description="new desc", details={"k": "v"})
        assert profile.description == "new desc"
        assert profile.details == {"k": "v"}

    def test_patch_not_found(self, rbac_service):
        rbac_service._channel_access_profile_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_channel_access_profile(uuid4(), name="x")


# ==================================================================================================
# Row access profiles CRUD
# ==================================================================================================


class TestRowAccessProfileCRUD:
    def _profile_mock(self, **kwargs):
        profile = MagicMock()
        profile.id = kwargs.get("id", uuid4())
        profile.name = kwargs.get("name", "row-profile")
        profile.description = kwargs.get("description", None)
        rid = kwargs.get("role_id", uuid4())
        profile.role_id = rid
        profile.role = _fake_role(id=rid, name=kwargs.get("role_name", "r"))
        row_id = kwargs.get("row_id", uuid4())
        profile.row_id = row_id
        row = MagicMock()
        row.id = row_id
        row.name = "row"
        profile.row = row
        return profile

    def test_create(self, rbac_service):
        profile = self._profile_mock(description="desc")
        rbac_service._ensure_row_access_profile = MagicMock(return_value=profile)

        with patch.object(OutputRowAccessProfile, "from_db", return_value=MagicMock(spec=OutputRowAccessProfile)):
            out = rbac_service.create_row_access_profile(
                profile_in=InputRowAccessProfile(
                    role=InputRole(id=uuid4()),
                    row=InputRow(id=uuid4(), channel=InputCoreChannel(id=uuid4())),
                    description="desc",
                )
            )
        assert isinstance(out, OutputRowAccessProfile)

    def test_list(self, rbac_service):
        profile = self._profile_mock()
        rbac_service._row_access_profile_repo.fetch_all = MagicMock(return_value=[profile])

        with patch.object(OutputRowAccessProfile, "from_db", return_value=MagicMock(spec=OutputRowAccessProfile)):
            result = rbac_service.list_row_access_profiles()
        assert len(result) == 1
        assert isinstance(result[0], OutputRowAccessProfile)

    def test_get(self, rbac_service):
        pid = uuid4()
        profile = self._profile_mock(id=pid)
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=profile)

        with patch.object(OutputRowAccessProfile, "from_db", return_value=MagicMock(spec=OutputRowAccessProfile)):
            result = rbac_service.get_row_access_profile(pid)
        assert isinstance(result, OutputRowAccessProfile)

    def test_get_none(self, rbac_service):
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=None)
        assert rbac_service.get_row_access_profile(uuid4()) is None

    def test_patch(self, rbac_service):
        pid = uuid4()
        profile = self._profile_mock(id=pid, name="old")
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=profile)
        rid = uuid4()
        role = _fake_role(id=rid, name="new-role")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)

        with patch.object(OutputRowAccessProfile, "from_db", return_value=MagicMock(spec=OutputRowAccessProfile)):
            rbac_service.patch_row_access_profile(
                pid, name="new", description="d", details={"k": "v"}, role=InputRole(id=rid)
            )
        assert profile.name == "new"
        assert profile.description == "d"
        assert profile.details == {"k": "v"}
        assert profile.role_id == rid

    def test_patch_not_found(self, rbac_service):
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_row_access_profile(uuid4(), name="x")

    def test_delete(self, rbac_service):
        pid = uuid4()
        profile = self._profile_mock(id=pid)
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=profile)

        with patch.object(OutputRowAccessProfile, "from_db", return_value=MagicMock(spec=OutputRowAccessProfile)):
            rbac_service.delete_row_access_profile(pid)
        db.delete.assert_called_once_with(profile)

    def test_delete_not_found(self, rbac_service):
        rbac_service._row_access_profile_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.delete_row_access_profile(uuid4())


# ==================================================================================================
# list_access_profiles
# ==================================================================================================


class TestListAccessProfiles:
    def test_list_access_profiles(self, rbac_service):
        rbac_service._zone_access_profile_repo.fetch_all = MagicMock(return_value=[])
        rbac_service._channel_access_profile_repo.fetch_all = MagicMock(return_value=[])
        rbac_service._row_access_profile_repo.fetch_all = MagicMock(return_value=[])

        result = rbac_service.list_access_profiles()
        assert "zone" in result
        assert "channel" in result
        assert "row" in result
        assert len(result["zone"]) == 0
        assert len(result["channel"]) == 0
        assert len(result["row"]) == 0


# ==================================================================================================
# _create_policy existing + default name
# ==================================================================================================


class TestCreatePolicyInternal:
    def test_existing_policy_returns_early(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        subj = MagicMock()
        subj.id = uuid4()
        subj.name = "subj"
        db_access = MagicMock()
        db_access.id = uuid4()
        db_access.name = "profile"

        policy_repo = MagicMock()
        existing_policy = MagicMock()
        policy_repo.get_by_subject_and_access_profile = MagicMock(return_value=existing_policy)

        output_cls = MagicMock()
        output_cls.from_db.return_value = MagicMock()

        result = rbac_service._create_policy(
            db,
            subj=subj,
            db_access=db_access,
            policy_repo=policy_repo,
            policy_cls=MagicMock(),
            output_cls=output_cls,
        )
        output_cls.from_db.assert_called_once_with(existing_policy)
        assert result is output_cls.from_db.return_value

    def test_default_name_generation(self, rbac_service):
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        subj = MagicMock()
        subj.id = uuid4()
        subj.name = "test-subject"
        db_access = MagicMock()
        db_access.id = uuid4()
        db_access.name = "Long Access Profile Name Here"
        db_access.role_id = uuid4()

        policy_repo = MagicMock()
        policy_repo.get_by_subject_and_access_profile = MagicMock(return_value=None)

        PolicyCls = MagicMock()
        OutputCls = MagicMock()
        policy_instance = MagicMock()
        policy_instance.name = None
        policy_instance.details = None
        PolicyCls.return_value = policy_instance
        OutputCls.from_db.return_value = MagicMock()

        result = rbac_service._create_policy(
            db,
            subj=subj,
            db_access=db_access,
            policy_repo=policy_repo,
            policy_cls=PolicyCls,
            output_cls=OutputCls,
        )
        PolicyCls.assert_called_once()
        call_kwargs = PolicyCls.call_args[1]
        assert "Long Access Profile Name" in call_kwargs["name"]
        assert "test-subject" in call_kwargs["name"]
        assert result is OutputCls.from_db.return_value


# ==================================================================================================
# patch zone/channel/row policies
# ==================================================================================================


class TestPatchZonePolicy:
    def test_patch_name_and_details(self, rbac_service):
        pid = uuid4()
        policy = MagicMock()
        policy.name = "old"
        policy.details = None
        rbac_service._zone_policy_repo.get_by_id = MagicMock(return_value=policy)

        with patch.object(OutputZonePolicy, "from_db", return_value=MagicMock(spec=OutputZonePolicy)):
            rbac_service.patch_zone_policy(pid, name="new", details={"k": "v"})
        assert policy.name == "new"
        assert policy.details == {"k": "v"}

    def test_patch_not_found(self, rbac_service):
        rbac_service._zone_policy_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_zone_policy(uuid4(), name="x")


class TestPatchChannelPolicy:
    def test_patch_name_and_details(self, rbac_service):
        pid = uuid4()
        policy = MagicMock()
        policy.name = "old"
        policy.details = None
        rbac_service._channel_policy_repo.get_by_id = MagicMock(return_value=policy)

        with patch.object(OutputChannelPolicy, "from_db", return_value=MagicMock(spec=OutputChannelPolicy)):
            rbac_service.patch_channel_policy(pid, name="new", details={"k": "v"})
        assert policy.name == "new"
        assert policy.details == {"k": "v"}

    def test_patch_not_found(self, rbac_service):
        rbac_service._channel_policy_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_channel_policy(uuid4(), name="x")


class TestRowPolicy:
    def test_create(self, rbac_service):
        sid, rid, row_id = uuid4(), uuid4(), uuid4()
        role = _fake_role(id=rid, name="role")
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)

        with patch.object(OutputRowPolicy, "from_db") as mock_from_db:
            expected = MagicMock()
            expected.role = MagicMock()
            expected.role.id = rid
            expected.subject = MagicMock()
            expected.subject.id = sid
            expected.access_profile = MagicMock()
            expected.access_profile.role = MagicMock()
            expected.access_profile.role.id = rid
            expected.access_profile.row = MagicMock()
            expected.access_profile.row.id = row_id
            mock_from_db.return_value = expected

            result = rbac_service.create_row_policy(
                subject=InputSubject(id=sid, type="user", user_id=sid),
                access_profile=InputRowAccessProfile(
                    role=InputRole(id=rid),
                    row=InputRow(id=row_id, channel=InputCoreChannel(id=uuid4())),
                ),
            )

            assert result.subject.id == sid
            assert result.access_profile.role.id == rid
            assert result.access_profile.row.id == row_id

    def test_list(self, rbac_service):
        row_id = uuid4()
        policy = MagicMock()
        policy.id = uuid4()
        policy.name = "rp"
        policy.subject = MagicMock()
        policy.access_profile = MagicMock()
        policy.access_profile.row = MagicMock()
        policy.access_profile.row.id = row_id
        policy.access_profile.role = _fake_role()
        policy.is_delegation = False
        rbac_service._row_policy_repo.get_policies_for_row = MagicMock(return_value=[policy])

        with patch.object(OutputRowPolicy, "from_db", return_value=MagicMock(spec=OutputRowPolicy)):
            result = rbac_service.list_policies_for_row(row_id)
        assert len(result) == 1

    def test_delete(self, rbac_service):
        pid = uuid4()
        policy = MagicMock()
        policy.id = pid
        db = rbac_service._db.transaction.return_value.__enter__.return_value
        rbac_service._row_policy_repo.get_by_id = MagicMock(return_value=policy)

        with patch.object(OutputRowPolicy, "from_db", return_value=MagicMock(spec=OutputRowPolicy)):
            rbac_service.delete_row_policy(pid)
        db.delete.assert_called_once_with(policy)

    def test_delete_not_found(self, rbac_service):
        rbac_service._row_policy_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.delete_row_policy(uuid4())

    def test_patch(self, rbac_service):
        pid = uuid4()
        policy = MagicMock()
        policy.name = "old"
        policy.details = None
        rbac_service._row_policy_repo.get_by_id = MagicMock(return_value=policy)

        with patch.object(OutputRowPolicy, "from_db", return_value=MagicMock(spec=OutputRowPolicy)):
            rbac_service.patch_row_policy(pid, name="new", details={"k": "v"})
        assert policy.name == "new"
        assert policy.details == {"k": "v"}

    def test_patch_not_found(self, rbac_service):
        rbac_service._row_policy_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError):
            rbac_service.patch_row_policy(uuid4(), name="x")


# ==================================================================================================
# add_row_read_policies
# ==================================================================================================


class TestAddRowReadPolicies:
    def test_no_users_no_groups_returns_early(self, rbac_service):
        rbac_service._row_repo.save = MagicMock()
        rbac_service.add_row_read_policies(uuid4(), [1, 2], read_users=None, read_groups=None)
        rbac_service._row_repo.save.assert_not_called()

    def test_creates_policies_for_users(self, rbac_service):
        channel_id = uuid4()
        ts_row_ids = [1]
        user_name = "alice"
        role = _fake_role(name="Reader")
        rbac_service._role_repo.get_by_name = MagicMock(return_value=role)
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._row_repo.save = MagicMock(return_value=MagicMock(id=uuid4()))
        rbac_service._row_access_profile_repo.create = MagicMock(
            return_value=MagicMock(id=uuid4(), name="row-ap", role=role)
        )
        subj = MagicMock()
        subj.id = uuid4()
        subj.name = user_name
        rbac_service._subject_repo.ensure_from_user = MagicMock(return_value=subj)
        rbac_service._subject_repo.ensure_from_group = MagicMock(return_value=subj)
        rbac_service._user_repo.get_by_name = MagicMock(return_value=_fake_user(name=user_name))
        policy_repo = MagicMock()
        policy_repo.get_by_subject_and_access_profile = MagicMock(return_value=None)
        rbac_service._row_policy_repo = policy_repo
        PolicyCls = MagicMock()
        PolicyCls.return_value = MagicMock(name=None, details=None)
        rbac_service._create_policy = MagicMock()

        rbac_service.add_row_read_policies(channel_id, ts_row_ids, read_users=[user_name], read_groups=None)
        rbac_service._create_policy.assert_called()

    def test_creates_policies_for_groups(self, rbac_service):
        channel_id = uuid4()
        ts_row_ids = [1]
        group_name = "team-a"
        role = _fake_role(name="Reader")
        rbac_service._role_repo.get_by_name = MagicMock(return_value=role)
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._row_repo.save = MagicMock(return_value=MagicMock(id=uuid4()))
        rbac_service._row_access_profile_repo.create = MagicMock(
            return_value=MagicMock(id=uuid4(), name="row-ap", role=role)
        )
        subj = MagicMock()
        subj.id = uuid4()
        subj.name = group_name
        rbac_service._subject_repo.ensure_from_group = MagicMock(return_value=subj)
        rbac_service._group_repo.get_by_name = MagicMock(return_value=_fake_group(name=group_name))
        rbac_service._create_policy = MagicMock()

        rbac_service.add_row_read_policies(channel_id, ts_row_ids, read_users=None, read_groups=[group_name])
        rbac_service._create_policy.assert_called()

    def test_skips_missing_users(self, rbac_service):
        channel_id = uuid4()
        ts_row_ids = [1]
        role = _fake_role(name="Reader")
        rbac_service._role_repo.get_by_name = MagicMock(return_value=role)
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._row_repo.save = MagicMock(return_value=MagicMock(id=uuid4()))
        rbac_service._row_access_profile_repo.create = MagicMock(
            return_value=MagicMock(id=uuid4(), name="row-ap", role=role)
        )
        rbac_service._user_repo.get_by_name = MagicMock(return_value=None)
        rbac_service._subject_repo.ensure_from_user = MagicMock(side_effect=NotFoundError("User 'missing' not found"))
        rbac_service._create_policy = MagicMock()

        rbac_service.add_row_read_policies(channel_id, ts_row_ids, read_users=["missing"], read_groups=None)
        rbac_service._create_policy.assert_not_called()

    def test_skips_missing_groups(self, rbac_service):
        channel_id = uuid4()
        ts_row_ids = [1]
        role = _fake_role(name="Reader")
        rbac_service._role_repo.get_by_name = MagicMock(return_value=role)
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._row_repo.save = MagicMock(return_value=MagicMock(id=uuid4()))
        rbac_service._row_access_profile_repo.create = MagicMock(
            return_value=MagicMock(id=uuid4(), name="row-ap", role=role)
        )
        rbac_service._group_repo.get_by_name = MagicMock(return_value=None)
        rbac_service._subject_repo.ensure_from_group = MagicMock(side_effect=NotFoundError("Group 'missing' not found"))
        rbac_service._create_policy = MagicMock()

        rbac_service.add_row_read_policies(channel_id, ts_row_ids, read_users=None, read_groups=["missing"])
        rbac_service._create_policy.assert_not_called()

    def test_multiple_rows(self, rbac_service):
        channel_id = uuid4()
        ts_row_ids = [1, 2]
        role = _fake_role(name="Reader")
        rbac_service._role_repo.get_by_name = MagicMock(return_value=role)
        rbac_service._role_repo.get_by_id = MagicMock(return_value=role)
        rbac_service._row_repo.save = MagicMock(return_value=MagicMock(id=uuid4()))
        rbac_service._row_access_profile_repo.create = MagicMock(
            return_value=MagicMock(id=uuid4(), name="row-ap", role=role)
        )
        subj = MagicMock()
        subj.id = uuid4()
        subj.name = "alice"
        rbac_service._subject_repo.ensure_from_user = MagicMock(return_value=subj)
        rbac_service._user_repo.get_by_name = MagicMock(return_value=_fake_user(name="alice"))
        rbac_service._create_policy = MagicMock()

        rbac_service.add_row_read_policies(channel_id, ts_row_ids, read_users=["alice"], read_groups=None)
        assert rbac_service._create_policy.call_count == 2


# ==================================================================================================
# list_policies (all types)
# ==================================================================================================


class TestListPoliciesAll:
    def test_list_zone_policies(self, rbac_service):
        rbac_service._zone_policy_repo.fetch_all = MagicMock(return_value=[])
        result = rbac_service.list_zone_policies()
        assert result == []

    def test_list_channel_policies(self, rbac_service):
        rbac_service._channel_policy_repo.fetch_all = MagicMock(return_value=[])
        result = rbac_service.list_channel_policies()
        assert result == []

    def test_list_row_policies(self, rbac_service):
        rbac_service._row_policy_repo.fetch_all = MagicMock(return_value=[])
        result = rbac_service.list_row_policies()
        assert result == []

    def test_list_policies(self, rbac_service):
        rbac_service._zone_policy_repo.fetch_all = MagicMock(return_value=[])
        rbac_service._channel_policy_repo.fetch_all = MagicMock(return_value=[])
        rbac_service._row_policy_repo.fetch_all = MagicMock(return_value=[])
        result = rbac_service.list_policies()
        assert "zone" in result
        assert "channel" in result
        assert "row" in result


# ==================================================================================================
# remove_user_from_group group not found
# ==================================================================================================


class TestRemoveUserFromGroupEdges:
    def test_remove_user_from_group_group_not_found(self, rbac_service):
        uid = uuid4()
        rbac_service._user_repo.get_by_id = MagicMock(return_value=_fake_user(id=uid))
        rbac_service._group_repo.get_by_id = MagicMock(return_value=None)
        with pytest.raises(NotFoundError, match="Group"):
            rbac_service.remove_user_from_group(uid, uuid4())


# ==================================================================================================
# list_role_subjects with descendants
# ==================================================================================================


class TestListRoleSubjectsDescendants:
    def test_indirect_with_descendants(self, rbac_service):
        rid = uuid4()
        uid, gid, desc_gid, member_uid = uuid4(), uuid4(), uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value

        all_results = [
            [(uid,)],
            [(gid,)],
            [(gid,)],
            [(desc_gid,)],
            [],
        ]
        db.query.return_value.filter.return_value.all.side_effect = all_results
        rbac_service._user_groups_repo.get_user_ids_for_group = MagicMock(return_value={member_uid})

        result = rbac_service.list_role_subjects(rid, indirect=True)
        assert str(uid) in result["users"]
        assert str(gid) in result["groups"]


# ==================================================================================================
# _check_policy_perm group match (line 421)
# ==================================================================================================


class TestCheckPolicyPermGroupMatch:
    def test_group_policy_perm_found_via_user_has_permission(self, rbac_service):
        uid, gid = uuid4(), uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value

        mock_direct = MagicMock()
        mock_direct.first.return_value = None
        mock_group_roles = MagicMock()
        mock_group_roles.first.return_value = None
        mock_zone_user = MagicMock()
        mock_zone_user.first.return_value = None
        mock_zone_group = MagicMock()
        mock_zone_group.first.return_value = (uuid4(),)

        db.execute.side_effect = [mock_direct, mock_group_roles, mock_zone_user, mock_zone_group]
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value={gid})

        result = rbac_service.user_has_permission(uid, "zone:read")
        assert result is True


# ==================================================================================================
# user_has_permission row policy (line 391)
# ==================================================================================================


class TestUserHasPermissionRowPolicy:
    def test_via_row_policy(self, rbac_service):
        uid = uuid4()
        db = rbac_service._db.get_db.return_value.__enter__.return_value

        mock_direct = MagicMock()
        mock_direct.first.return_value = None
        mock_zone = MagicMock()
        mock_zone.first.return_value = None
        mock_channel = MagicMock()
        mock_channel.first.return_value = None
        mock_row = MagicMock()
        mock_row.first.return_value = (uuid4(),)

        db.execute.side_effect = [mock_direct, mock_zone, mock_channel, mock_row]
        rbac_service._user_groups_repo.get_group_ids_for_user = MagicMock(return_value=set())

        result = rbac_service.user_has_permission(uid, "zone:read")
        assert result is True


# ==================================================================================================
# patch_zone_policy extra edge
# ==================================================================================================


class TestPatchZonePolicyEdges:
    def test_patch_details_only(self, rbac_service):
        pid = uuid4()
        policy = MagicMock()
        policy.name = "keep"
        policy.details = {"old": True}
        rbac_service._zone_policy_repo.get_by_id = MagicMock(return_value=policy)

        with patch.object(OutputZonePolicy, "from_db", return_value=MagicMock(spec=OutputZonePolicy)):
            rbac_service.patch_zone_policy(pid, details={"new": True})
        assert policy.details == {"new": True}
        assert policy.name == "keep"


class TestPatchChannelPolicyEdges:
    def test_patch_details_only(self, rbac_service):
        pid = uuid4()
        policy = MagicMock()
        policy.name = "keep"
        policy.details = {"old": True}
        rbac_service._channel_policy_repo.get_by_id = MagicMock(return_value=policy)

        with patch.object(OutputChannelPolicy, "from_db", return_value=MagicMock(spec=OutputChannelPolicy)):
            rbac_service.patch_channel_policy(pid, details={"new": True})
        assert policy.details == {"new": True}
        assert policy.name == "keep"


# ==================================================================================================
# patch_row_policy extra edge
# ==================================================================================================


class TestPatchRowPolicyEdges:
    def test_patch_details_only(self, rbac_service):
        pid = uuid4()
        policy = MagicMock()
        policy.name = "keep"
        policy.details = {"old": True}
        rbac_service._row_policy_repo.get_by_id = MagicMock(return_value=policy)

        with patch.object(OutputRowPolicy, "from_db", return_value=MagicMock(spec=OutputRowPolicy)):
            rbac_service.patch_row_policy(pid, details={"new": True})
        assert policy.details == {"new": True}
        assert policy.name == "keep"
