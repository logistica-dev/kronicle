# tests/unit/services/test_seed_service.py
"""Unit tests for kronicle.services.seed_service."""

from unittest.mock import MagicMock, patch

from kronicle.deps.rbac_defaults import ANONYMOUS_NAME
from kronicle.deps.settings_env import AppSuperuser
from kronicle.services import seed_service


def _rbac_db():
    rbac_db = MagicMock()
    rbac_db.transaction.return_value.__enter__.return_value = MagicMock(name="db")
    return rbac_db


def test_seed_default_roles_creates_missing_roles():
    rbac_db = _rbac_db()
    defaults = [
        {"name": "reader", "description": "read", "permissions": ["read"]},
        {"name": "writer", "description": "write", "permissions": ["write"], "restrictions": ["drop"]},
    ]
    with (
        patch.object(seed_service, "DEFAULT_ROLES", defaults),
        patch.object(seed_service, "RbacRoleRepository") as repo_cls,
    ):
        role_repo = repo_cls.return_value
        role_repo.get_by_name.return_value = None
        seed_service.seed_default_roles(rbac_db)

    assert role_repo.add.call_count == 2
    first = role_repo.add.call_args_list[0].kwargs["entity"]
    second = role_repo.add.call_args_list[1].kwargs["entity"]
    assert isinstance(first, seed_service.RbacRole)
    assert first.name == "reader"
    assert first.permissions == ["read"]
    assert first.details == {"seed": True}
    assert second.name == "writer"
    assert second.restrictions == ["drop"]
    assert role_repo.get_by_name.call_args_list[0].kwargs["name"] == "reader"


def test_seed_default_roles_skips_existing_role():
    existing = MagicMock()
    rbac_db = _rbac_db()
    with (
        patch.object(seed_service, "DEFAULT_ROLES", [{"name": "reader", "permissions": ["read"]}]),
        patch.object(seed_service, "RbacRoleRepository") as repo_cls,
    ):
        role_repo = repo_cls.return_value
        role_repo.get_by_name.return_value = existing
        seed_service.seed_default_roles(rbac_db)

    role_repo.add.assert_not_called()


def test_seed_default_roles_skips_existing_but_creates_new():
    existing = MagicMock()
    rbac_db = _rbac_db()
    defaults = [
        {"name": "reader", "permissions": ["read"]},
        {"name": "auditor", "permissions": ["audit"]},
    ]
    with (
        patch.object(seed_service, "DEFAULT_ROLES", defaults),
        patch.object(seed_service, "RbacRoleRepository") as repo_cls,
    ):
        role_repo = repo_cls.return_value
        role_repo.get_by_name.side_effect = [existing, None]
        seed_service.seed_default_roles(rbac_db)

    assert role_repo.add.call_count == 1
    created = role_repo.add.call_args.kwargs["entity"]
    assert created.name == "auditor"
    assert created.details == {"seed": True}


def test_seed_anonymous_group_creates_when_allowed():
    rbac_db = _rbac_db()
    with (
        patch.object(seed_service, "RbacGroupRepository") as group_cls,
        patch.object(seed_service, "RbacSubjectRepository") as subject_cls,
    ):
        group_repo = group_cls.return_value
        subject_repo = subject_cls.return_value
        group_repo.get_by_name.return_value = None
        seed_service.seed_anonymous_group(rbac_db, allow_anonymous=True)

    group = group_repo.add.call_args.kwargs["entity"]
    assert isinstance(group, seed_service.RbacGroup)
    assert group.name == ANONYMOUS_NAME
    assert group.details == {"seed": True}
    subject_repo.ensure_from_group.assert_called_once()
    assert subject_repo.ensure_from_group.call_args.kwargs["group"] is group


def test_seed_anonymous_group_skips_when_allowed_and_exists():
    rbac_db = _rbac_db()
    with (
        patch.object(seed_service, "RbacGroupRepository") as group_cls,
        patch.object(seed_service, "RbacSubjectRepository") as subject_cls,
    ):
        group_repo = group_cls.return_value
        group_repo.get_by_name.return_value = MagicMock()
        seed_service.seed_anonymous_group(rbac_db, allow_anonymous=True)

    group_repo.add.assert_not_called()
    subject_cls.return_value.ensure_from_group.assert_not_called()


def test_seed_anonymous_group_deletes_when_disallowed_and_exists():
    group = MagicMock()
    rbac_db = _rbac_db()
    with (
        patch.object(seed_service, "RbacGroupRepository") as group_cls,
        patch.object(seed_service, "RbacSubjectRepository") as subject_cls,
    ):
        group_repo = group_cls.return_value
        group_repo.get_by_name.return_value = group
        seed_service.seed_anonymous_group(rbac_db, allow_anonymous=False)

    db = rbac_db.transaction.return_value.__enter__.return_value
    group_repo.add.assert_not_called()
    group_repo.delete.assert_called_once_with(db, entity=group)
    subject_cls.return_value.ensure_from_group.assert_not_called()


def test_seed_anonymous_group_noop_when_disallowed_and_missing():
    rbac_db = _rbac_db()
    with patch.object(seed_service, "RbacGroupRepository") as group_cls:
        group_repo = group_cls.return_value
        group_repo.get_by_name.return_value = None
        seed_service.seed_anonymous_group(rbac_db, allow_anonymous=False)

    group_repo.add.assert_not_called()
    group_repo.delete.assert_not_called()


def test_seed_app_superuser_skips_existing_superuser():
    superuser = MagicMock()
    superuser.is_superuser = True
    rbac_db = _rbac_db()
    su = AppSuperuser(username="root", email="root@kronicle.app", password_hash="hash")

    with patch.object(seed_service, "RbacUserRepository") as repo_cls:
        repo_cls.return_value.get_by_name.return_value = superuser
        seed_service.seed_app_superuser(rbac_db, su)

    repo_cls.return_value.add.assert_not_called()
    assert superuser.is_superuser is True


def test_seed_app_superuser_promotes_existing_user():
    superuser = MagicMock()
    superuser.is_superuser = False
    rbac_db = _rbac_db()
    su = AppSuperuser(username="root", email="root@kronicle.app", password_hash="hash")

    with patch.object(seed_service, "RbacUserRepository") as repo_cls:
        repo_cls.return_value.get_by_name.return_value = superuser
        seed_service.seed_app_superuser(rbac_db, su)

    assert superuser.is_superuser is True
    repo_cls.return_value.add.assert_not_called()


def test_seed_app_superuser_creates_new_user():
    rbac_db = _rbac_db()
    su = AppSuperuser(username="root", email="root@kronicle.app", password_hash="hash")

    with patch.object(seed_service, "RbacUserRepository") as repo_cls:
        repo_cls.return_value.get_by_name.return_value = None
        seed_service.seed_app_superuser(rbac_db, su)

    db = rbac_db.transaction.return_value.__enter__.return_value
    user_repo = repo_cls.return_value
    user_repo.get_by_name.assert_called_once_with(db, name="root", include_superusers=True)
    user = user_repo.add.call_args.kwargs["entity"]
    assert isinstance(user, seed_service.RbacUser)
    assert user.name == "root"
    assert user.email == "root@kronicle.app"
    assert user.password_hash == "hash"
    assert user.is_active is True
    assert user.is_superuser is True
