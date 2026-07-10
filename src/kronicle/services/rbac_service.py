# kronicle/services/rbac_service.py
from __future__ import annotations

import functools
from typing import Any, Final, Sequence
from uuid import UUID

from sqlalchemy import cast, func, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.session import Session

from kronicle.db.core.models.core_channel import CoreChannel
from kronicle.db.core.models.core_row import CoreRow
from kronicle.db.core.models.core_zone import CoreZone
from kronicle.db.rbac.links.group_hierarchy import RbacGroupHierarchy
from kronicle.db.rbac.links.group_roles import RbacGroupRoles
from kronicle.db.rbac.links.rbac_access_profile import ChannelAccessProfile, RowAccessProfile, ZoneAccessProfile
from kronicle.db.rbac.links.rbac_policy import ChannelPolicy, RowPolicy, ZonePolicy
from kronicle.db.rbac.links.user_groups import RbacUserGroups
from kronicle.db.rbac.links.user_roles import RbacUserRoles
from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_role import RbacRole
from kronicle.db.rbac.models.rbac_subject import RbacSubject
from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.db.rbac.rbac_db_session import RbacDbSession
from kronicle.errors.error_types import BadRequestError, ConflictError, NotFoundError, UnauthorizedError
from kronicle.repo.core.core_channel_repo import CoreChannelRepository
from kronicle.repo.core.core_row_repo import CoreRowRepository
from kronicle.repo.core.core_zone_repo import CoreZoneRepository
from kronicle.repo.hierarchy.hierarchy_engine import HierarchyEngine
from kronicle.repo.hierarchy.hierarchy_service import HierarchyService
from kronicle.repo.rbac.entities.channel_policy_repo import ChannelPolicyRepository
from kronicle.repo.rbac.entities.rbac_group_repo import RbacGroupRepository
from kronicle.repo.rbac.entities.rbac_role_repo import RbacRoleRepository
from kronicle.repo.rbac.entities.rbac_subject_repo import RbacSubjectRepository
from kronicle.repo.rbac.entities.rbac_user_repo import RbacUserRepository
from kronicle.repo.rbac.entities.row_policy_repo import RowPolicyRepository
from kronicle.repo.rbac.entities.zone_policy_repo import ZonePolicyRepository
from kronicle.repo.rbac.links.channel_access_profile_repo import ChannelAccessProfileRepository
from kronicle.repo.rbac.links.rbac_group_roles_repo import RbacGroupRolesRepository
from kronicle.repo.rbac.links.rbac_user_group_repo import RbacUserGroupRepository
from kronicle.repo.rbac.links.rbac_user_roles_repo import RbacUserRolesRepository
from kronicle.repo.rbac.links.row_access_profile_repo import RowAccessProfileRepository
from kronicle.repo.rbac.links.zone_access_profile_repo import ZoneAccessProfileRepository
from kronicle.schemas.core.input_ressource_schema import InputZonePatch
from kronicle.schemas.payload.input_payload import InputPayload
from kronicle.schemas.permissions.permission import Permission
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
    OutputAccessProfile,
    OutputChannelAccessProfile,
    OutputChannelPolicy,
    OutputRowAccessProfile,
    OutputRowPolicy,
    OutputZoneAccessProfile,
    OutputZonePolicy,
)
from kronicle.schemas.rbac.safe_role_schemas import OutputRole
from kronicle.schemas.rbac.safe_user_schemas import OutputUser, ProcessedUser
from kronicle.utils.dev_logs import log_d, log_i, log_w

mod = "rbacs"

ANONYMOUS_NAME: Final[str] = "anonymous"
RESERVED_NAMES = ["superuser", "admin", ANONYMOUS_NAME]


def log_service_error(method):
    """Log the exception with the method name and re-raise."""

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception as e:
            log_w(method.__name__, type(e).__name__, str(e))
            raise

    return wrapper


"""
FastAPI validates inputs.
RbacService determines transaction scope.
RbacDbSession provides session/connection context.
RbacEngine orchestrates multi-table actions using the session.
Table classes perform simple CRUD and return results.
"""


class RbacService:
    def __init__(
        self,
        rbac_db_session: RbacDbSession,
        reserved_names: list[str] | None = None,
    ):
        self._reserved_names = [n.lower() for n in (reserved_names or RESERVED_NAMES)]
        self._db = rbac_db_session

        # Rbac objects
        self._user_repo = RbacUserRepository()
        self._group_repo = RbacGroupRepository()
        self._role_repo = RbacRoleRepository()
        self._subject_repo = RbacSubjectRepository()

        # Core objects
        self._channel_repo = CoreChannelRepository()
        self._zone_repo = CoreZoneRepository()
        self._row_repo = CoreRowRepository()

        # Rbac links
        self._user_groups_repo = RbacUserGroupRepository()
        self._user_roles_repo = RbacUserRolesRepository()
        self._group_roles_repo = RbacGroupRolesRepository()

        # Rbac <-> core links
        self._zone_access_profile_repo = ZoneAccessProfileRepository()
        self._channel_access_profile_repo = ChannelAccessProfileRepository()
        self._row_access_profile_repo = RowAccessProfileRepository()
        self._zone_policy_repo = ZonePolicyRepository()
        self._channel_policy_repo = ChannelPolicyRepository()
        self._row_policy_repo = RowPolicyRepository()

        group_engine = HierarchyEngine(
            parents_of=lambda g: g.parent_links,
            children_of=lambda g: g.children,
        )

        self.group_hierarchy_service = HierarchyService(
            engine=group_engine,
            add_edge=RbacGroupHierarchy.add,
            remove_edge=RbacGroupHierarchy.remove,
            max_parents=5,
        )

        self._ensure_anonymous_group()

    def _ensure_anonymous_group(self) -> None:
        """Create the 'Anonymous' group if it doesn't exist."""
        with self._db.transaction() as db:
            existing = self._group_repo.get_by_name(db, name=ANONYMOUS_NAME)
            if existing:
                return
            group = RbacGroup(name=ANONYMOUS_NAME)
            db.add(group)
            db.flush()
            self._subject_repo.ensure_from_group(db, group=group)

    # ----------------------------------------------------------------------------------------------
    # Read-only: fetch user info
    # ----------------------------------------------------------------------------------------------
    def _fetch_user_by_login(self, login_input: InputUserLogin) -> RbacUser:
        with self._db.get_db() as db:  # read-only
            # TODO: remove this!
            users = self._user_repo.fetch_all(db, include_superusers=True)
            log_d(mod, "_fetch_user_by_login", "users:", users)
            if login_input.is_email:
                email = f"{login_input.login}".lower()
                db_user = self._user_repo.get_by_email(db, email=email, include_superusers=True)
            else:
                name = login_input.login
                db_user = self._user_repo.get_by_name(db, name=name, include_superusers=True)
        if not db_user:
            raise NotFoundError("User not found")
        return db_user

    def fetch_user_for_auth(self, login_input: InputUserLogin) -> RbacUser:
        return self._fetch_user_by_login(login_input)

    def fetch_user_info(self, login_input: InputUserLogin) -> OutputUser:
        """
        Fetch the OutputUser for the authenticated user.
        Should only be called after login.
        """
        db_user = self._fetch_user_by_login(login_input)
        return OutputUser.from_db(db_user)

    def fetch_user_by_email(self, email: str) -> OutputUser | None:
        with self._db.get_db() as db:  # read-only
            db_user = self._user_repo.get_by_email(db, email=email)
            return OutputUser.from_db(db_user) if db_user else None

    def fetch_user_by_name(self, name: str) -> OutputUser | None:
        with self._db.get_db() as db:  # read-only
            db_user = self._user_repo.get_by_name(db, name=name)
            return OutputUser.from_db(db_user) if db_user else None

    def fetch_user_by_id(self, id: UUID) -> OutputUser | None:
        with self._db.get_db() as db:  # read-only
            db_user = self._user_repo.get_by_id(db, id=id)
            return OutputUser.from_db(db_user) if db_user else None

    def fetch_user_by_external_id(self, orcid: str) -> OutputUser | None:
        with self._db.get_db() as db:  # read-only
            db_user = self._user_repo.get_by_external_id(db, external_id=orcid)
            return OutputUser.from_db(db_user) if db_user else None

    def list_users(self, *, include_inactive: bool = False) -> list[OutputUser]:
        here = mod + ".list_users"
        log_d(here, "include_inactive", include_inactive)
        with self._db.get_db() as db:  # read-only
            users = self._user_repo.fetch_all(db, include_inactive=include_inactive)
        return [OutputUser.from_db(u) for u in users]

    # ----------------------------------------------------------------------------------------------
    # Write: create user
    # ----------------------------------------------------------------------------------------------
    def _assert_name_not_reserved(self, name: str) -> None:
        if name.lower() in self._reserved_names:
            raise BadRequestError(f"The name '{name}' is reserved and cannot be used.")

    def _assert_user_name_available(self, db: Session, name: str) -> None:
        self._assert_name_not_reserved(name)
        existing = self._group_repo.get_by_name(db, name=name)
        if existing:
            raise BadRequestError(f"A group named '{name}' already exists. User and group names must be unique.")

    def _assert_group_name_available(self, db: Session, name: str) -> None:
        self._assert_name_not_reserved(name)
        existing = self._user_repo.get_by_name(db, name=name)
        if existing:
            raise BadRequestError(f"A user named '{name}' already exists. Group and user names must be unique.")

    def create_user(self, user: ProcessedUser) -> OutputUser:
        here = "create_usr"
        log_d(here, user.email)
        if not user.password_hash:
            raise BadRequestError("Input user password should be provided")
        rbac_user = user.to_db_user()

        with self._db.transaction() as db:
            self._assert_user_name_available(db, rbac_user.name)
            existing = self._user_repo.get_by_email(db=db, email=rbac_user.email)
            if existing:
                raise UnauthorizedError(f"Email already in use: {rbac_user.email}")
            log_d(here, rbac_user.name)
            db_user = self._user_repo.create_user(db=db, user=rbac_user)
        out_user = OutputUser.from_db(db_user)
        return out_user

    def patch_user(self, user: ProcessedUser) -> OutputUser:
        here = "patch_user"
        log_i(here, user.email)
        with self._db.transaction() as db:
            db_user: RbacUser = self._user_repo.get_by_email(db=db, email=user.email)
            if not db_user:
                raise UnauthorizedError("User doesn't exists")
            updated = False
            if user.name is not None and user.name != db_user.name:
                self._assert_user_name_available(db, user.name)
                db_user.name = user.name
                updated = True
            if user.full_name is not None:
                db_user.full_name = user.full_name
                updated = True
            if user.external_id is not None:
                db_user.external_id = user.external_id
                updated = True
            if updated:
                try:
                    db.commit()
                except IntegrityError as e:
                    log_w(here, "IntegrityError", e)
                    raise UnauthorizedError("Attempting to patch with existing values") from e

            db.refresh(db_user)
        return OutputUser.from_db(db_user)

    def patch_user_by_id(
        self,
        user_id: UUID,
        name: str | None = None,
        full_name: str | None = None,
        orcid: str | None = None,
    ) -> OutputUser:
        here = "patch_user_by_id"
        log_i(here, user_id)
        with self._db.transaction() as db:
            db_user = self._user_repo.get_by_id(db, id=user_id)
            if not db_user:
                raise NotFoundError(f"User '{user_id}' not found")
            updated = False
            if name is not None and name != db_user.name:
                self._assert_user_name_available(db, name)
                db_user.name = name
                updated = True
            if full_name is not None:
                db_user.full_name = full_name
                updated = True
            if orcid is not None:
                db_user.external_id = orcid
                updated = True
            if updated:
                try:
                    db.commit()
                except IntegrityError as e:
                    log_w(here, "IntegrityError", e)
                    raise BadRequestError("Attempting to patch with existing values") from e
            db.refresh(db_user)
        return OutputUser.from_db(db_user)

    def update_password_hash(self, user_id: UUID, new_hash: str) -> None:
        with self._db.transaction() as db:
            self._user_repo.update_password_hash(db, user_id=user_id, new_hash=new_hash)

    # ----------------------------------------------------------------------------------------------
    # Write: delete (deactivate/remove) user
    # ----------------------------------------------------------------------------------------------

    def _deactivate_user(self, db: Session, db_user: RbacUser | None) -> OutputUser:
        here = "_deact_usr"
        if not db_user:
            raise UnauthorizedError("User doesn't exists")
        log_i(here, db_user.model_dump)
        db_user.is_active = False
        db.commit()
        db.refresh(db_user)
        return OutputUser.from_db(db_user)

    def _delete_user(self, db: Session, db_user: RbacUser | None) -> OutputUser:
        here = "_del_usr"
        if not db_user:
            raise UnauthorizedError("User doesn't exists")
        log_w(here, db_user.model_dump)

        # Clear role and group assignments explicitly — even with ondelete=CASCADE,
        # SQLAlchemy's unitofwork processor tries to NULL PK columns before
        # the DB-level cascade, causing an AssertionError.
        self._user_roles_repo.delete_all_for_user(db, user_id=db_user.id)
        self._user_groups_repo.delete_all_for_user(db, user_id=db_user.id)

        deleted_user = self._user_repo.delete_user(db, user=db_user)
        db.commit()
        return OutputUser.from_db(deleted_user)

    def deactivate_user(self, user: ProcessedUser) -> OutputUser:
        with self._db.transaction() as db:
            db_user: RbacUser = self._user_repo.get_by_email(db=db, email=user.email, include_inactive=True)
            return self._deactivate_user(db, db_user)

    def deactivate_user_by_id(self, id: UUID) -> OutputUser:
        with self._db.transaction() as db:
            db_user: RbacUser = self._user_repo.get_by_id(db=db, id=id, include_inactive=True)
            return self._deactivate_user(db, db_user)

    def remove_user(self, user: ProcessedUser) -> OutputUser:
        with self._db.transaction() as db:
            db_user: RbacUser = self._user_repo.get_by_email(db=db, email=user.email, include_inactive=True)
            return self._delete_user(db, db_user)

    def remove_user_by_id(self, id: UUID) -> OutputUser:
        with self._db.transaction() as db:
            db_user: RbacUser = self._user_repo.get_by_id(db=db, id=id, include_inactive=True)
            return self._delete_user(db, db_user)

    # ----------------------------------------------------------------------------------------------
    # Subjects / Groups
    # ----------------------------------------------------------------------------------------------
    def get_user_groups(self, user_id: UUID) -> list[UUID]:
        """
        Returns a list of group IDs the user belongs to.
        """
        with self._db.get_db() as db:
            return list(self._user_groups_repo.get_group_ids_for_user(db, user_id=user_id))

    # ----------------------------------------------------------------------------------------------
    # Permissions
    # ----------------------------------------------------------------------------------------------
    def _user_has_permission_via_policy(
        self, db: Session, user_id: UUID | None, group_ids: list[UUID], perm_jsonb: Any
    ) -> bool:
        """Check if the user has a permission through a policy (zone/channel/row)."""

        # Zone policies
        if self._check_policy_perm(db, ZonePolicy, ZoneAccessProfile, user_id, group_ids, perm_jsonb):
            return True
        # Channel policies
        if self._check_policy_perm(db, ChannelPolicy, ChannelAccessProfile, user_id, group_ids, perm_jsonb):
            return True
        # Row policies
        if self._check_policy_perm(db, RowPolicy, RowAccessProfile, user_id, group_ids, perm_jsonb):
            return True
        return False

    def _check_policy_perm(
        self, db: Session, policy_cls, profile_cls, user_id: UUID | None, group_ids: list[UUID], perm_jsonb: Any
    ) -> bool:
        """Check permission via one policy type (zone, channel, or row)."""

        # Subject = user directly
        found = db.execute(
            select(policy_cls.id)
            .join(RbacSubject, RbacSubject.id == policy_cls.subject_id)
            .join(profile_cls, profile_cls.id == policy_cls.access_profile_id)
            .join(RbacRole, RbacRole.id == profile_cls.role_id)
            .where(RbacSubject.user_id == user_id)
            .where(RbacRole.permissions.op("@>")(perm_jsonb))
        ).first()
        if found:
            return True
        # Subject = one of the user's groups
        if group_ids:
            found = db.execute(
                select(policy_cls.id)
                .join(RbacSubject, RbacSubject.id == policy_cls.subject_id)
                .join(profile_cls, profile_cls.id == policy_cls.access_profile_id)
                .join(RbacRole, RbacRole.id == profile_cls.role_id)
                .where(RbacSubject.group_id.in_(group_ids))
                .where(RbacRole.permissions.op("@>")(perm_jsonb))
            ).first()
            if found:
                return True
        return False

    def user_has_permission(self, user_id: UUID | None, permission: str | Permission) -> bool:
        perm_str = str(permission) if isinstance(permission, Permission) else permission
        Permission.parse(perm_str)
        with self._db.get_db() as db:
            perm_jsonb = cast(func.json_build_array(perm_str), JSONB)

            # Anonymous user — only check the "anonymous" group via policies
            if user_id is None:
                anonymous = self._group_repo.get_by_name(db, name=ANONYMOUS_NAME)
                if not anonymous:
                    return False
                return self._user_has_permission_via_policy(db, None, [anonymous.id], perm_jsonb)

            # Direct user roles
            has_direct = db.execute(
                select(RbacUserRoles.role_id)
                .join(RbacRole, RbacRole.id == RbacUserRoles.role_id)
                .where(RbacUserRoles.user_id == user_id)
                .where(RbacRole.permissions.op("@>")(perm_jsonb))
            ).first()
            if has_direct:
                return True
            # Group roles
            group_ids = self._user_groups_repo.get_group_ids_for_user(db, user_id=user_id)
            if group_ids:
                has_group = db.execute(
                    select(RbacGroupRoles.role_id)
                    .join(RbacRole, RbacRole.id == RbacGroupRoles.role_id)
                    .where(RbacGroupRoles.group_id.in_(group_ids))
                    .where(RbacRole.permissions.op("@>")(perm_jsonb))
                ).first()
                if has_group:
                    return True
            # Policies (zone/channel/row)
            return self._user_has_permission_via_policy(db, user_id, list(group_ids) or [], perm_jsonb)

    # ----------------------------------------------------------------------------------------------
    # User ↔ Role assignment
    # ----------------------------------------------------------------------------------------------

    def assign_role_to_user(self, user_id: UUID, role_id: UUID) -> None:
        with self._db.transaction() as db:
            self._user_roles_repo.assign_role_to_user(db, user_id=user_id, role_id=role_id)

    def remove_role_from_user(self, user_id: UUID, role_id: UUID) -> None:
        with self._db.transaction() as db:
            self._user_roles_repo.remove_role_from_user(db, user_id=user_id, role_id=role_id)

    # ----------------------------------------------------------------------------------------------
    # Group ↔ Role assignment
    # ----------------------------------------------------------------------------------------------

    def assign_role_to_group(self, group_id: UUID, role_id: UUID) -> None:
        with self._db.transaction() as db:
            self._group_roles_repo.assign_role_to_group(db, group_id=group_id, role_id=role_id)

    def remove_role_from_group(self, group_id: UUID, role_id: UUID) -> None:
        with self._db.transaction() as db:
            self._group_roles_repo.remove_role_from_group(db, group_id=group_id, role_id=role_id)

    # ----------------------------------------------------------------------------------------------
    # Roles
    # ----------------------------------------------------------------------------------------------
    def create_role(
        self,
        name: str,
        description: str | None = None,
        permissions: list[str] | None = None,
        restrictions: list[str] | None = None,
        details: dict | None = None,
    ) -> OutputRole:
        here = "create_role"
        log_d(here, name)

        role = RbacRole(
            name=name,
            description=description or "",
            permissions=permissions or [],
            restrictions=restrictions or [],
            details=details or {},
        )
        with self._db.transaction() as db:
            existing = self._role_repo.get_by_name(db, name=name)
            if existing:
                raise BadRequestError(f"Role '{name}' already exists")
            db.add(role)
            db.flush()
            return OutputRole.from_db(role)

    def get_roles(self) -> list[OutputRole]:
        with self._db.get_db() as db:
            roles = self._role_repo.fetch_all(db)
            return [OutputRole.from_db(r) for r in roles]

    def get_role(self, role_id: UUID) -> OutputRole | None:
        with self._db.get_db() as db:
            role = self._role_repo.get_by_id(db, id=role_id)
            return OutputRole.from_db(role) if role else None

    def get_role_by_name(self, name: str) -> OutputRole | None:
        with self._db.get_db() as db:
            role = self._role_repo.get_by_name(db, name=name)
            return OutputRole.from_db(role) if role else None

    def patch_role(
        self,
        role_id: UUID,
        name: str | None = None,
        description: str | None = None,
        permissions: list[str] | None = None,
        restrictions: list[str] | None = None,
        details: dict | None = None,
    ) -> OutputRole:
        here = "patch_role"
        log_d(here, role_id)
        with self._db.transaction() as db:
            role = self._role_repo.get_by_id(db, id=role_id)
            if not role:
                raise NotFoundError(f"Role '{role_id}' not found")
            if name is not None:
                role.name = name
            if description is not None:
                role.description = description
            if permissions is not None:
                role.permissions = permissions
            if restrictions is not None:
                role.restrictions = restrictions
            if details is not None:
                role.details = details
            db.flush()
            db.refresh(role)
            return OutputRole.from_db(role)

    def delete_role(self, role_id: UUID, force: bool = False) -> OutputRole | None:
        here = "delete_role"
        log_w(here, role_id)
        with self._db.transaction() as db:
            role = self._role_repo.get_by_id(db, id=role_id)
            if not role:
                raise NotFoundError(f"Role '{role_id}' not found")

            if not force:
                users_stmt = (
                    select(RbacUser)
                    .join(RbacUserRoles, RbacUser.id == RbacUserRoles.user_id)
                    .where(RbacUserRoles.role_id == role_id)
                )
                role_users = db.execute(users_stmt).scalars().all()

                groups_stmt = (
                    select(RbacGroup)
                    .join(RbacGroupRoles, RbacGroup.id == RbacGroupRoles.group_id)
                    .where(RbacGroupRoles.role_id == role_id)
                )
                role_groups = db.execute(groups_stmt).scalars().all()

                if role_users or role_groups:
                    details: dict[str, Any] = {"role": f"{role}"}
                    if role_users:
                        details["users"] = [u.snapshot for u in role_users]
                    if role_groups:
                        details["groups"] = [g.snapshot for g in role_groups]
                    raise ConflictError(
                        f"Role '{role.name}' cannot be deleted: {len(role_users)} user(s) and {len(role_groups)} group(s) still assigned.",
                        details=details,
                    )

            # Clear link rows explicitly — SQLAlchemy's unit of work can't
            # handle PK-as-FK with ondelete=CASCADE (tries to NULL the PK first).
            self._user_roles_repo.delete_all_for_role(db, role_id=role_id)
            self._group_roles_repo.delete_all_for_role(db, role_id=role_id)

            db.delete(role)
            db.flush()
            return OutputRole.from_db(role)

    # ----------------------------------------------------------------------------------------------
    # Policies
    # ----------------------------------------------------------------------------------------------
    def _ensure_subject_by_id(self, db: Session, subject: InputSubject) -> RbacSubject:
        if subject.type == "user":
            if not subject.user_id:
                raise BadRequestError("user_id must be provided for user subject")
            user = self._user_repo.get_by_id(db, id=subject.user_id, include_inactive=True)
            if not user:
                raise NotFoundError(f"User '{subject.user_id}' not found")
            if not user.is_active:
                raise UnauthorizedError(
                    message=f"User '{user.id}' is inactive and cannot be used as a subject of a policy.",
                    details={"user_id": user.id, "user_name": user.name},
                )
            return self._subject_repo.ensure_from_user(db, user=user)

        if not subject.group_id:
            raise BadRequestError("group_id must be provided for group subject")
        group = self._group_repo.get_by_id(db, id=subject.group_id)
        if not group:
            raise NotFoundError(f"Group '{subject.group_id}' not found")
        return self._subject_repo.ensure_from_group(db, group=group)

    def _ensure_subject_by_name(self, db: Session, subject_type: str, subject_name: str) -> RbacSubject:
        if subject_type == "user":
            user = self._user_repo.get_by_name(db, name=subject_name)
            if not user:
                raise NotFoundError(f"User '{subject_name}' not found")
            return self._subject_repo.ensure_from_user(db, user=user)

        group = self._group_repo.get_by_name(db, name=subject_name)
        if not group:
            raise NotFoundError(f"Group '{subject_name}' not found")
        return self._subject_repo.ensure_from_group(db, group=group)

    def _ensure_subject(self, db: Session, subject: InputSubject) -> RbacSubject:
        """Resolve an InputSubject ref to an RbacSubject (creating it if needed)."""

        if subject.id:
            return self._ensure_subject_by_id(db, subject)

        if not subject.name:
            raise BadRequestError("Either id or name must be provided for subject")
        return self._ensure_subject_by_name(db, subject_type=subject.type, subject_name=subject.name)

    def _ensure_zone_access_profile(self, db: Session, access_profile: InputZoneAccessProfile) -> ZoneAccessProfile:
        """Resolve a ZoneAccessProfile by id or name, or create it from the full input."""
        if access_profile.id:
            existing = self._zone_access_profile_repo.get_by_id(db, id=access_profile.id)
            if existing:
                return existing
        if access_profile.name:
            existing = self._zone_access_profile_repo.get_by_name(db, name=access_profile.name)
            if existing:
                return existing
        db_role = self._resolve_role(db, access_profile.role)
        db_zone = self._resolve_zone(db, access_profile.zone)
        existing = self._zone_access_profile_repo.get_by_role_and_zone(db, role_id=db_role.id, zone_id=db_zone.id)
        if existing:
            return existing
        name = access_profile.name or None
        if not name:
            name = f"Zone {db_zone.name[:15]} {db_role.name[:15]} access"
        profile = self._zone_access_profile_repo.create(db, role_id=db_role.id, zone_id=db_zone.id, name=name)
        if access_profile.description is not None:
            profile.description = access_profile.description
        if access_profile.details is not None:
            profile.details = access_profile.details
        db.flush()
        return profile

    def _ensure_channel_access_profile(
        self, db: Session, access_profile: InputChannelAccessProfile
    ) -> ChannelAccessProfile:
        """Resolve a ChannelAccessProfile by id or name, or create it from the full input."""
        if access_profile.id:
            existing = self._channel_access_profile_repo.get_by_id(db, id=access_profile.id)
            if existing:
                return existing
        if access_profile.name:
            existing = self._channel_access_profile_repo.get_by_name(db, name=access_profile.name)
            if existing:
                return existing
        db_role = self._resolve_role(db, access_profile.role)
        db_channel = self._resolve_channel(db, access_profile.channel)
        name = access_profile.name or None
        if not name:
            channel_name = str(db_channel.name)
            if channel_name.startswith("channel_"):
                channel_name = channel_name[8:]
            name = f"Channel {channel_name[:15]} {db_role.name[:15]} access"
        profile = self._channel_access_profile_repo.create(db, role_id=db_role.id, channel_id=db_channel.id, name=name)
        if access_profile.description is not None:
            profile.description = access_profile.description
        if access_profile.details is not None:
            profile.details = access_profile.details
        db.flush()
        return profile

    def _ensure_row_access_profile(self, db: Session, access_profile: InputRowAccessProfile) -> RowAccessProfile:
        """Resolve a RowAccessProfile by id or name, or create it from the full input."""
        if access_profile.id:
            existing = self._row_access_profile_repo.get_by_id(db, id=access_profile.id)
            if existing:
                return existing
        if access_profile.name:
            existing = self._row_access_profile_repo.get_by_name(db, name=access_profile.name)
            if existing:
                return existing
        db_role = self._resolve_role(db, access_profile.role)
        name = access_profile.name or None
        if not name:
            row_id = access_profile.row.id.hex
            name = f"Row {row_id[:15]} {db_role.name[:15]} access"
        profile = self._row_access_profile_repo.create(db, role_id=db_role.id, row_id=access_profile.row.id, name=name)
        if access_profile.description is not None:
            profile.description = access_profile.description
        if access_profile.details is not None:
            profile.details = access_profile.details
        db.flush()
        return profile

    # ----------------------------------------------------------------------------------------------
    # Reference resolution helpers
    # ----------------------------------------------------------------------------------------------

    def _resolve_role(self, db: Session, ref: InputRole) -> RbacRole:
        if ref.id:
            db_role = self._role_repo.get_by_id(db, id=ref.id)
            if not db_role:
                raise NotFoundError(f"Role '{ref.id}' not found")
            return db_role
        if not ref.name:
            raise BadRequestError("Either id or name must be provided for role")
        db_role = self._role_repo.get_by_name(db, name=ref.name)
        if not db_role:
            raise NotFoundError(f"Role '{ref.name}' not found")
        return db_role

    def _resolve_zone(self, db: Session, ref: InputZonePatch) -> CoreZone:
        if ref.id:
            db_zone = self._zone_repo.get_by_id(db, id=ref.id)
            if not db_zone:
                raise NotFoundError(f"Zone '{ref.id}' not found")
            return db_zone
        if not ref.name:
            raise BadRequestError("Either id or name must be provided for zone")
        db_zone = self._zone_repo.get_by_name(db, name=ref.name)
        if not db_zone:
            raise NotFoundError(f"Zone '{ref.name}' not found")
        return db_zone

    def _resolve_channel(self, db: Session, ref: InputPayload) -> CoreChannel:
        if ref.id:
            db_channel = self._channel_repo.get_by_id(db, id=ref.id)
            if not db_channel:
                raise NotFoundError(f"Channel '{ref.id}' not found")
            return db_channel
        if not ref.name:
            raise BadRequestError("Either id or name must be provided for channel")
        db_channel = self._channel_repo.get_by_name(db, name=ref.name)
        if not db_channel:
            raise NotFoundError(f"Channel '{ref.name}' not found")
        return db_channel

    # ----------------------------------------------------------------------------------------------
    # Access Profiles
    # ----------------------------------------------------------------------------------------------

    def create_zone_access_profile(self, *, profile_in: InputZoneAccessProfile) -> OutputZoneAccessProfile:
        with self._db.transaction() as db:
            profile = self._ensure_zone_access_profile(db, profile_in)
            return OutputZoneAccessProfile.from_db(profile)

    def list_zone_access_profiles(self) -> list[OutputZoneAccessProfile]:
        with self._db.get_db() as db:
            return [OutputZoneAccessProfile.from_db(p) for p in self._zone_access_profile_repo.fetch_all(db)]

    def get_zone_access_profile(self, profile_id: UUID) -> OutputZoneAccessProfile | None:
        with self._db.get_db() as db:
            p = self._zone_access_profile_repo.get_by_id(db, id=profile_id)
            return OutputZoneAccessProfile.from_db(p) if p else None

    def patch_zone_access_profile(
        self,
        profile_id: UUID,
        name: str | None = None,
        description: str | None = None,
        details: dict | None = None,
        role: InputRole | None = None,
    ) -> OutputZoneAccessProfile:
        with self._db.transaction() as db:
            profile = self._zone_access_profile_repo.get_by_id(db, id=profile_id)
            if not profile:
                raise NotFoundError(f"ZoneAccessProfile '{profile_id}' not found")
            if name is not None:
                profile.name = name
            if description is not None:
                profile.description = description
            if details is not None:
                profile.details = details
            if role is not None:
                db_role = self._resolve_role(db, role)
                profile.role_id = db_role.id
            db.flush()
            db.refresh(profile)
            return OutputZoneAccessProfile.from_db(profile)

    def delete_zone_access_profile(self, profile_id: UUID) -> OutputZoneAccessProfile:
        with self._db.transaction() as db:
            profile = self._zone_access_profile_repo.get_by_id(db, id=profile_id)
            if not profile:
                raise NotFoundError(f"ZoneAccessProfile '{profile_id}' not found")
            db.delete(profile)
            db.flush()
            return OutputZoneAccessProfile.from_db(profile)

    def create_channel_access_profile(self, *, profile_in: InputChannelAccessProfile) -> OutputChannelAccessProfile:
        with self._db.transaction() as db:
            profile = self._ensure_channel_access_profile(db, profile_in)
            return OutputChannelAccessProfile.from_db(profile)

    def list_channel_access_profiles(self) -> list[OutputChannelAccessProfile]:
        with self._db.get_db() as db:
            return [OutputChannelAccessProfile.from_db(p) for p in self._channel_access_profile_repo.fetch_all(db)]

    def get_channel_access_profile(self, profile_id: UUID) -> OutputChannelAccessProfile | None:
        with self._db.get_db() as db:
            profile = self._channel_access_profile_repo.get_by_id(db, id=profile_id)
            return OutputChannelAccessProfile.from_db(profile) if profile else None

    def patch_channel_access_profile(
        self,
        profile_id: UUID,
        name: str | None = None,
        description: str | None = None,
        details: dict | None = None,
        role: InputRole | None = None,
    ) -> OutputChannelAccessProfile:
        with self._db.transaction() as db:
            profile = self._channel_access_profile_repo.get_by_id(db, id=profile_id)
            if not profile:
                raise NotFoundError(f"ChannelAccessProfile '{profile_id}' not found")
            if name is not None:
                profile.name = name
            if description is not None:
                profile.description = description
            if details is not None:
                profile.details = details
            if role is not None:
                db_role = self._resolve_role(db, role)
                profile.role_id = db_role.id
            db.flush()
            db.refresh(profile)
            return OutputChannelAccessProfile.from_db(profile)

    def delete_channel_access_profile(self, profile_id: UUID) -> OutputChannelAccessProfile:
        with self._db.transaction() as db:
            profile = self._channel_access_profile_repo.get_by_id(db, id=profile_id)
            if not profile:
                raise NotFoundError(f"ChannelAccessProfile '{profile_id}' not found")
            db.delete(profile)
            db.flush()
            return OutputChannelAccessProfile.from_db(profile)

    def create_row_access_profile(self, *, profile_in: InputRowAccessProfile) -> OutputRowAccessProfile:
        with self._db.transaction() as db:
            profile = self._ensure_row_access_profile(db, profile_in)
            return OutputRowAccessProfile.from_db(profile)

    def list_row_access_profiles(self) -> list[OutputRowAccessProfile]:
        with self._db.get_db() as db:
            return [OutputRowAccessProfile.from_db(p) for p in self._row_access_profile_repo.fetch_all(db)]

    def get_row_access_profile(self, profile_id: UUID) -> OutputRowAccessProfile | None:
        with self._db.get_db() as db:
            profile = self._row_access_profile_repo.get_by_id(db, id=profile_id)
            return OutputRowAccessProfile.from_db(profile) if profile else None

    def patch_row_access_profile(
        self,
        profile_id: UUID,
        name: str | None = None,
        description: str | None = None,
        details: dict | None = None,
        role: InputRole | None = None,
    ) -> OutputRowAccessProfile:
        with self._db.transaction() as db:
            profile = self._row_access_profile_repo.get_by_id(db, id=profile_id)
            if not profile:
                raise NotFoundError(f"RowAccessProfile '{profile_id}' not found")
            if name is not None:
                profile.name = name
            if description is not None:
                profile.description = description
            if details is not None:
                profile.details = details
            if role is not None:
                db_role = self._resolve_role(db, role)
                profile.role_id = db_role.id
            db.flush()
            db.refresh(profile)
            return OutputRowAccessProfile.from_db(profile)

    def delete_row_access_profile(self, profile_id: UUID) -> OutputRowAccessProfile:
        with self._db.transaction() as db:
            profile = self._row_access_profile_repo.get_by_id(db, id=profile_id)
            if not profile:
                raise NotFoundError(f"RowAccessProfile '{profile_id}' not found")
            db.delete(profile)
            db.flush()
            return OutputRowAccessProfile.from_db(profile)

    def list_access_profiles(self) -> dict[str, Sequence[OutputAccessProfile]]:
        return {
            "zone": self.list_zone_access_profiles(),
            "channel": self.list_channel_access_profiles(),
            "row": self.list_row_access_profiles(),
        }

    # ----------------------------------------------------------------------------------------------
    # Policies: Zone level
    # ----------------------------------------------------------------------------------------------

    def _create_policy(
        self,
        db: Session,
        *,
        subj: RbacSubject,
        db_access,
        policy_repo,
        policy_cls,
        output_cls,
        name: str | None = None,
        details: dict | None = None,
    ):
        existing = policy_repo.get_by_subject_and_access_profile(db, subject_id=subj.id, access_profile_id=db_access.id)
        if existing:
            return output_cls.from_db(existing)

        if not name:
            name = f"{db_access.name[:44]} for {subj.name[:15]}"

        policy = policy_cls(
            subject_id=subj.id,
            access_profile_id=db_access.id,
            name=name,
            details=details,
        )
        db.add(policy)
        db.flush()
        db.refresh(policy)
        return output_cls.from_db(policy)

    def create_zone_policy(
        self,
        subject: InputSubject,
        access_profile: InputZoneAccessProfile,
        name: str | None = None,
        details: dict | None = None,
    ) -> OutputZonePolicy:
        """Assign a role to a subject (user or group) for a specific zone."""
        with self._db.transaction() as db:
            db_access = self._ensure_zone_access_profile(db, access_profile)
            subj = self._ensure_subject(db, subject)
            return self._create_policy(
                db,
                subj=subj,
                db_access=db_access,
                policy_repo=self._zone_policy_repo,
                policy_cls=ZonePolicy,
                output_cls=OutputZonePolicy,
                name=name,
                details=details,
            )

    def list_policies_for_zone(self, zone_id: UUID) -> list[OutputZonePolicy]:
        """List all policies for a zone."""
        with self._db.get_db() as db:
            policies = self._zone_policy_repo.get_policies_for_zone(db, zone_id=zone_id)
            return [OutputZonePolicy.from_db(p) for p in policies]

    def delete_zone_policy(self, policy_id: UUID) -> OutputZonePolicy:
        """Delete a zone policy by ID."""

        with self._db.transaction() as db:
            policy = self._zone_policy_repo.get_by_id(db, id=policy_id)
            if not policy:
                raise NotFoundError(f"ZonePolicy '{policy_id}' not found")
            db.delete(policy)
            db.flush()
            return OutputZonePolicy.from_db(policy)

    def patch_zone_policy(
        self, policy_id: UUID, name: str | None = None, details: dict | None = None
    ) -> OutputZonePolicy:
        """Patch a zone policy's name or details."""
        with self._db.transaction() as db:
            policy = self._zone_policy_repo.get_by_id(db, id=policy_id)
            if not policy:
                raise NotFoundError(f"ZonePolicy '{policy_id}' not found")
            if name is not None:
                policy.name = name
            if details is not None:
                policy.details = details
            db.flush()
            db.refresh(policy)
            return OutputZonePolicy.from_db(policy)

    # ----------------------------------------------------------------------------------------------
    # Policies: Channel level
    # ----------------------------------------------------------------------------------------------

    def create_channel_policy(
        self,
        subject: InputSubject,
        access_profile: InputChannelAccessProfile,
        name: str | None = None,
        details: dict | None = None,
    ) -> OutputChannelPolicy:
        """Assign a role to a subject (user or group) for a specific channel."""
        with self._db.transaction() as db:
            db_access = self._ensure_channel_access_profile(db, access_profile)
            subj = self._ensure_subject(db, subject)
            return self._create_policy(
                db,
                subj=subj,
                db_access=db_access,
                policy_repo=self._channel_policy_repo,
                policy_cls=ChannelPolicy,
                output_cls=OutputChannelPolicy,
                name=name,
                details=details,
            )

    def list_policies_for_channel(self, channel_id: UUID) -> list[OutputChannelPolicy]:
        """List all policies for a channel."""

        with self._db.get_db() as db:
            policies = self._channel_policy_repo.get_policies_for_channel(db, channel_id=channel_id)
            return [OutputChannelPolicy.from_db(p) for p in policies]

    def delete_channel_policy(self, policy_id: UUID) -> OutputChannelPolicy:
        """Delete a channel policy by ID."""

        with self._db.transaction() as db:
            policy = self._channel_policy_repo.get_by_id(db, id=policy_id)
            if not policy:
                raise NotFoundError(f"ChannelPolicy '{policy_id}' not found")
            db.delete(policy)
            db.flush()
            return OutputChannelPolicy.from_db(policy)

    def patch_channel_policy(
        self, policy_id: UUID, name: str | None = None, details: dict | None = None
    ) -> OutputChannelPolicy:
        """Patch a channel policy's name or details."""
        with self._db.transaction() as db:
            policy = self._channel_policy_repo.get_by_id(db, id=policy_id)
            if not policy:
                raise NotFoundError(f"ChannelPolicy '{policy_id}' not found")
            if name is not None:
                policy.name = name
            if details is not None:
                policy.details = details
            db.flush()
            db.refresh(policy)
            return OutputChannelPolicy.from_db(policy)

    # ----------------------------------------------------------------------------------------------
    # Policies: Row level
    # ----------------------------------------------------------------------------------------------

    def add_row_read_policies(
        self,
        channel_id: UUID,
        timeseries_row_ids: list[int],
        read_users: list[str] | None = None,
        read_groups: list[str] | None = None,
    ) -> None:
        """Create row-level read policies for inserted rows.

        For each row, creates a CoreRow record, a RowAccessProfile with the
        "Reader" role, and RowPolicy entries for each named user / group.
        Silently skips subjects that don't exist yet.
        """
        users = read_users or []
        groups = read_groups or []
        if not users and not groups:
            return

        reader_role = InputRole(name="Reader")

        with self._db.transaction() as db:
            db_role = self._resolve_role(db, reader_role)

            for ts_row_id in timeseries_row_ids:
                core_row = self._row_repo.save(
                    db,
                    entity=CoreRow(
                        timeseries_row_id=ts_row_id,
                        channel_id=channel_id,
                        name=f"row_{ts_row_id}",
                    ),
                )
                row_ap = self._row_access_profile_repo.create(
                    db,
                    role_id=db_role.id,
                    row_id=core_row.id,
                    name=f"Row {ts_row_id} {db_role.name} access",
                )

                for uname in users:
                    try:
                        subj = self._ensure_subject_by_name(db, "user", uname)
                        self._create_policy(
                            db,
                            subj=subj,
                            db_access=row_ap,
                            policy_repo=self._row_policy_repo,
                            policy_cls=RowPolicy,
                            output_cls=OutputRowPolicy,
                            name=f"{row_ap.name[:44]} for {subj.name[:15]}",
                        )
                    except NotFoundError:
                        continue

                for gname in groups:
                    try:
                        subj = self._ensure_subject_by_name(db, "group", gname)
                        self._create_policy(
                            db,
                            subj=subj,
                            db_access=row_ap,
                            policy_repo=self._row_policy_repo,
                            policy_cls=RowPolicy,
                            output_cls=OutputRowPolicy,
                            name=f"{row_ap.name[:44]} for {subj.name[:15]}",
                        )
                    except NotFoundError:
                        continue

    def create_row_policy(
        self,
        subject: InputSubject,
        access_profile: InputRowAccessProfile,
        name: str | None = None,
        details: dict | None = None,
    ) -> OutputRowPolicy:
        """Assign a role to a subject (user or group) for a specific row."""
        with self._db.transaction() as db:
            db_access = self._ensure_row_access_profile(db, access_profile)
            subj = self._ensure_subject(db, subject)
            return self._create_policy(
                db,
                subj=subj,
                db_access=db_access,
                policy_repo=self._row_policy_repo,
                policy_cls=RowPolicy,
                output_cls=OutputRowPolicy,
                name=name,
                details=details,
            )

    def list_policies_for_row(self, row_id: UUID) -> list[OutputRowPolicy]:
        """List all policies for a row."""
        with self._db.get_db() as db:
            policies = self._row_policy_repo.get_policies_for_row(db, row_id=row_id)
            return [OutputRowPolicy.from_db(p) for p in policies]

    def delete_row_policy(self, policy_id: UUID) -> OutputRowPolicy:
        """Delete a row policy by ID."""
        with self._db.transaction() as db:
            policy = self._row_policy_repo.get_by_id(db, id=policy_id)
            if not policy:
                raise NotFoundError(f"RowPolicy '{policy_id}' not found")
            db.delete(policy)
            db.flush()
            return OutputRowPolicy.from_db(policy)

    def patch_row_policy(
        self, policy_id: UUID, name: str | None = None, details: dict | None = None
    ) -> OutputRowPolicy:
        """Patch a row policy's name or details."""
        with self._db.transaction() as db:
            policy = self._row_policy_repo.get_by_id(db, id=policy_id)
            if not policy:
                raise NotFoundError(f"RowPolicy '{policy_id}' not found")
            if name is not None:
                policy.name = name
            if details is not None:
                policy.details = details
            db.flush()
            db.refresh(policy)
            return OutputRowPolicy.from_db(policy)

    # ----------------------------------------------------------------------------------------------
    # List all policies (by type)
    # ----------------------------------------------------------------------------------------------

    def list_zone_policies(self) -> list[OutputZonePolicy]:
        """List all zone policies regardless of zone."""
        with self._db.get_db() as db:
            return [OutputZonePolicy.from_db(p) for p in self._zone_policy_repo.fetch_all(db)]

    def list_channel_policies(self) -> list[OutputChannelPolicy]:
        """List all channel policies regardless of channel."""
        with self._db.get_db() as db:
            return [OutputChannelPolicy.from_db(p) for p in self._channel_policy_repo.fetch_all(db)]

    def list_row_policies(self) -> list[OutputRowPolicy]:
        """List all row policies regardless of row."""
        with self._db.get_db() as db:
            return [OutputRowPolicy.from_db(p) for p in self._row_policy_repo.fetch_all(db)]

    def list_policies(self) -> dict:
        """Return all policies across all resource types as a dict."""
        return {
            "zone": self.list_zone_policies(),
            "channel": self.list_channel_policies(),
            "row": self.list_row_policies(),
        }

    # ----------------------------------------------------------------------------------------------
    # Groups
    # ----------------------------------------------------------------------------------------------
    def create_group(self, name: str, details: dict | None = None) -> OutputGroup:
        here = "create_group"
        log_d(here, name)
        group = RbacGroup(name=name, details=details or {})
        with self._db.transaction() as db:
            self._assert_group_name_available(db, name)
            existing = self._group_repo.get_by_name(db, name=name)
            if existing:
                raise BadRequestError(f"Group '{name}' already exists")
            db.add(group)
            db.flush()
            return OutputGroup.from_db(group)

    def get_groups(self) -> list[OutputGroup]:
        with self._db.get_db() as db:
            groups = self._group_repo.fetch_all(db)
            return [OutputGroup.from_db(g) for g in groups]

    def get_group_by_id(self, group_id: UUID) -> OutputGroup | None:
        with self._db.get_db() as db:
            group = self._group_repo.get_by_id(db, id=group_id)
            return OutputGroup.from_db(group) if group else None

    def get_group_by_name(self, name: str) -> OutputGroup | None:
        with self._db.get_db() as db:
            group = self._group_repo.get_by_name(db, name=name)
            return OutputGroup.from_db(group) if group else None

    def get_users_from_group(self, *, group_id: UUID) -> list[OutputUser]:
        list_users = []
        with self._db.get_db() as db:
            user_id_list = self._user_groups_repo.get_user_ids_for_group(db, group_id=group_id)
            for u_id in user_id_list:
                usr = self._user_repo.get_by_id(db, id=u_id)
                if usr:
                    list_users.append(OutputUser.from_db(usr))
        return list_users

    def patch_group(self, group_id: UUID, name: str | None = None, details: dict | None = None) -> OutputGroup:
        here = "patch_group"
        log_d(here, group_id)
        with self._db.transaction() as db:
            group: RbacGroup | None = self._group_repo.get_by_id(db, id=group_id)
            if not group:
                raise NotFoundError(f"Group '{group_id}' not found")
            if name is not None:
                group.name = name
            if details is not None:
                group.details = details
            db.flush()
            db.refresh(group)
            return OutputGroup.from_db(group)

    @log_service_error
    def delete_group(self, group_id: UUID, force: bool = False) -> OutputGroup | None:
        here = "delete_group"
        log_w(here, group_id)
        with self._db.transaction() as db:
            group = self._group_repo.get_by_id(db, id=group_id)
            if not group:
                raise NotFoundError(f"Group '{group_id}' not found")

            if not force:
                stmt = (
                    select(RbacUser)
                    .join(RbacUserGroups, RbacUser.id == RbacUserGroups.user_id)
                    .where(RbacUserGroups.group_id == group_id)
                )
                group_users = db.execute(stmt).scalars().all()

                if group_users:
                    raise ConflictError(
                        f"Group '{group.name}' cannot be deleted: {len(group_users)} user(s) still assigned.",
                        details={"group": f"{group}", "users": [u.snapshot for u in group_users]},
                    )

            # Remove role assignments (clean up links instead of blocking)
            self._group_roles_repo.delete_all_for_group(db, group_id=group_id)

            # Clear group memberships explicitly — SQLAlchemy's unitofwork can't
            # handle PK-as-FK with ondelete=CASCADE (tries to NULL the PK first).
            self._user_groups_repo.delete_all_for_group(db, group_id=group_id)

            db.delete(group)
            db.flush()
            return OutputGroup.from_db(group)

    def add_user_to_group(self, user_id: UUID, group_id: UUID) -> None:
        here = "add_user_to_group"
        log_d(here, user_id, group_id)
        with self._db.transaction() as db:
            user = self._user_repo.get_by_id(db, id=user_id)
            if not user:
                raise NotFoundError(f"User '{user_id}' not found")
            group = self._group_repo.get_by_id(db, id=group_id)
            if not group:
                raise NotFoundError(f"Group '{group_id}' not found")
            self._user_groups_repo.add_user_to_group(db, user=user, group=group)

    def remove_user_from_group(self, user_id: UUID, group_id: UUID) -> None:
        here = "remove_user_from_group"
        log_d(here, user_id, group_id)
        with self._db.transaction() as db:
            user = self._user_repo.get_by_id(db, id=user_id)
            if not user:
                raise NotFoundError(f"User '{user_id}' not found")
            group = self._group_repo.get_by_id(db, id=group_id)
            if not group:
                raise NotFoundError(f"Group '{group_id}' not found")
            self._user_groups_repo.remove_user_from_group(db, user=user, group=group)

    # ----------------------------------------------------------------------------------------------
    # Hierarchy helpers
    # ----------------------------------------------------------------------------------------------

    def _get_group_ancestor_ids(self, db: Session, group_id: UUID) -> set[UUID]:
        """Walk up the group hierarchy to find all ancestor group IDs."""
        ancestors: set[UUID] = set()
        stack = [group_id]
        while stack:
            current = stack.pop()
            rows = db.query(RbacGroupHierarchy.parent_id).filter(RbacGroupHierarchy.child_id == current).all()
            for (parent_id,) in rows:
                if parent_id not in ancestors:
                    ancestors.add(parent_id)
                    stack.append(parent_id)
        return ancestors

    def _get_group_descendant_ids(self, db: Session, group_id: UUID) -> set[UUID]:
        """Walk down the group hierarchy to find all descendant group IDs."""
        descendants: set[UUID] = set()
        stack = [group_id]
        while stack:
            current = stack.pop()
            rows = db.query(RbacGroupHierarchy.child_id).filter(RbacGroupHierarchy.parent_id == current).all()
            for (child_id,) in rows:
                if child_id not in descendants:
                    descendants.add(child_id)
                    stack.append(child_id)
        return descendants

    # ----------------------------------------------------------------------------------------------
    # Relationship checks
    # ----------------------------------------------------------------------------------------------

    def check_user_has_role(self, user_id: UUID, role_id: UUID, indirect: bool = False) -> dict:
        with self._db.get_db() as db:
            direct = db.query(RbacUserRoles.__table__).filter_by(user_id=user_id, role_id=role_id).first() is not None
            if direct:
                return {"has_role": True, "direct": True}
            if not indirect:
                return {"has_role": False, "direct": False}
            user_group_ids = self._user_groups_repo.get_group_ids_for_user(db, user_id=user_id)
            all_group_ids = set(user_group_ids)
            for gid in user_group_ids:
                all_group_ids.update(self._get_group_ancestor_ids(db, gid))
            has_via_group = (
                db.query(RbacGroupRoles.__table__)
                .filter(RbacGroupRoles.group_id.in_(all_group_ids), RbacGroupRoles.role_id == role_id)
                .first()
                is not None
            )
            return {"has_role": has_via_group, "direct": False}

    def check_group_has_role(self, group_id: UUID, role_id: UUID, indirect: bool = False) -> dict:
        with self._db.get_db() as db:
            direct = (
                db.query(RbacGroupRoles.__table__).filter_by(group_id=group_id, role_id=role_id).first() is not None
            )
            if direct:
                return {"has_role": True, "direct": True}
            if not indirect:
                return {"has_role": False, "direct": False}
            ancestor_ids = self._get_group_ancestor_ids(db, group_id)
            has_via_ancestor = (
                db.query(RbacGroupRoles.__table__)
                .filter(RbacGroupRoles.group_id.in_(ancestor_ids), RbacGroupRoles.role_id == role_id)
                .first()
                is not None
            )
            return {"has_role": has_via_ancestor, "direct": False}

    def list_role_subjects(self, role_id: UUID, indirect: bool = False) -> dict:
        with self._db.get_db() as db:
            user_ids = list(db.query(RbacUserRoles.user_id).filter(RbacUserRoles.role_id == role_id).all())
            user_ids = [str(u[0]) for u in user_ids]

            group_ids = list(db.query(RbacGroupRoles.group_id).filter(RbacGroupRoles.role_id == role_id).all())
            group_ids = [str(g[0]) for g in group_ids]

            if not indirect:
                return {"users": user_ids, "groups": group_ids}

            indirect_user_ids: set[str] = set()
            for (gid,) in db.query(RbacGroupRoles.group_id).filter(RbacGroupRoles.role_id == role_id).all():
                members = self._user_groups_repo.get_user_ids_for_group(db, group_id=gid)
                indirect_user_ids.update(str(m) for m in members)
                descendant_ids = self._get_group_descendant_ids(db, gid)
                for desc_id in descendant_ids:
                    members = self._user_groups_repo.get_user_ids_for_group(db, group_id=desc_id)
                    indirect_user_ids.update(str(m) for m in members)

            direct_user_set = set(user_ids)
            indirect_only = sorted(indirect_user_ids - direct_user_set)
            return {
                "users": sorted(user_ids),
                "groups": sorted(group_ids),
                "indirect_users": indirect_only,
            }

    def check_user_in_group(self, user_id: UUID, group_id: UUID, indirect: bool = False) -> dict:
        with self._db.get_db() as db:
            direct = (
                db.query(RbacUserGroups.__table__).filter_by(user_id=user_id, group_id=group_id).first() is not None
            )
            if direct:
                return {"is_member": True, "direct": True}
            if not indirect:
                return {"is_member": False, "direct": False}
            descendant_ids = self._get_group_descendant_ids(db, group_id)
            for desc_id in descendant_ids:
                members = self._user_groups_repo.get_user_ids_for_group(db, group_id=desc_id)
                if user_id in members:
                    return {"is_member": True, "direct": False}
            return {"is_member": False, "direct": False}
