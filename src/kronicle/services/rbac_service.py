# kronicle/services/rbac_service.py
from __future__ import annotations

import functools
from typing import Any, Sequence
from uuid import UUID

from sqlalchemy import cast, func, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.session import Session

from kronicle.db.core.links.zone_hierarchy import ZoneHierarchy
from kronicle.db.core.models.core_channel import CoreChannel
from kronicle.db.core.models.core_row import CoreRow
from kronicle.db.core.models.core_zone import CoreZone
from kronicle.db.rbac.links.group_hierarchy import RbacGroupHierarchy
from kronicle.db.rbac.links.group_roles import RbacGroupRoles
from kronicle.db.rbac.links.rbac_access_profile import ChannelAccessProfile, RowAccessProfile, ZoneAccessProfile
from kronicle.db.rbac.links.rbac_link import RbacLink
from kronicle.db.rbac.links.rbac_policy import ChannelPolicy, RowPolicy, ZonePolicy
from kronicle.db.rbac.links.user_groups import RbacUserGroups
from kronicle.db.rbac.links.user_roles import RbacUserRoles
from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_role import RbacRole
from kronicle.db.rbac.models.rbac_subject import RbacSubject
from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.db.rbac.rbac_db_session import RbacDbSession
from kronicle.deps.rbac_defaults import ANONYMOUS_NAME, RESERVED_NAMES
from kronicle.errors.error_types import BadRequestError, ConflictError, NotFoundError, UnauthorizedError
from kronicle.repo.core.core_channel_repo import CoreChannelRepository
from kronicle.repo.core.core_row_repo import CoreRowRepository
from kronicle.repo.core.core_zone_repo import CoreZoneRepository
from kronicle.repo.hierarchy.hierarchy_service import HierarchyService
from kronicle.repo.hierarchy.rbac_group_hierarchy_repo import RbacGroupHierarchyRepository
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
from kronicle.schemas.output_schema import OutputSchema
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
from kronicle.schemas.rbac.safe_introspect_schemas import OutputGroupPermissions, OutputUserPermissions, ResourceAccess
from kronicle.schemas.rbac.safe_link_schemas import (
    OutputGroupRole,
    OutputRoleSubjects,
    OutputUserGroupMembership,
    OutputUserRole,
)
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

        self.group_hierarchy_service = HierarchyService(
            repo=RbacGroupHierarchyRepository(),
            max_parents=5,
        )

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

        # Subject = user directly (skip for anonymous — user_id is None)
        if user_id is not None:
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

    def assign_role_to_user(self, user_id: UUID, role_id: UUID) -> OutputUserRole | None:
        with self._db.transaction() as db:
            link = self._user_roles_repo.assign_role_to_user(db, user_id=user_id, role_id=role_id)
            return OutputUserRole.from_db(link) if link else None

    def remove_role_from_user(self, user_id: UUID, role_id: UUID) -> OutputUserRole | None:
        with self._db.transaction() as db:
            link = self._user_roles_repo.remove_role_from_user(db, user_id=user_id, role_id=role_id)
            return OutputUserRole.from_db(link) if link else None

    # ----------------------------------------------------------------------------------------------
    # Group ↔ Role assignment
    # ----------------------------------------------------------------------------------------------

    def assign_role_to_group(self, group_id: UUID, role_id: UUID) -> OutputGroupRole | None:
        with self._db.transaction() as db:
            link = self._group_roles_repo.assign_role_to_group(db, group_id=group_id, role_id=role_id)
            return OutputGroupRole.from_db(link) if link else None

    def remove_role_from_group(self, group_id: UUID, role_id: UUID) -> OutputGroupRole | None:
        with self._db.transaction() as db:
            link = self._group_roles_repo.remove_role_from_group(db, group_id=group_id, role_id=role_id)
            return OutputGroupRole.from_db(link) if link else None

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
            self._role_repo.add(db, entity=role)
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

            role = self._role_repo.delete(db, entity=role)
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
        existing = self._channel_access_profile_repo.get_by_role_and_channel(
            db, role_id=db_role.id, channel_id=db_channel.id
        )
        if existing:
            return existing
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

    def _ensure_core_row(self, db: Session, *, channel_id: UUID, timeseries_row_id: int) -> CoreRow:
        """Find a CoreRow for (channel_id, timeseries_row_id) or create it lazily."""
        row = self._row_repo.get_by_channel_and_row_id(
            db,
            channel_id=channel_id,
            timeseries_row_id=timeseries_row_id,
        )
        if row:
            return row
        row = CoreRow(
            timeseries_row_id=timeseries_row_id,
            channel_id=channel_id,
            name=f"row_{timeseries_row_id}",
        )
        db.add(row)
        db.flush()
        return row

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
        db_row = self._ensure_core_row(
            db,
            channel_id=access_profile.row.channel_id,
            timeseries_row_id=access_profile.row.id,
        )
        name = access_profile.name or None
        if not name:
            row_id = db_row.id.hex
            name = f"Row {row_id[:15]} {db_role.name[:15]} access"
        profile = self._row_access_profile_repo.create(db, role_id=db_role.id, row_id=db_row.id, name=name)
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
            profile = self._zone_access_profile_repo.delete(db, entity=profile)
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
            profile = self._channel_access_profile_repo.delete(db, entity=profile)
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
            profile = self._row_access_profile_repo.delete(db, entity=profile)
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
        policy = policy_repo.add(db, entity=policy)
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

    def get_zone_policy(self, policy_id: UUID) -> OutputZonePolicy:
        """Get a zone policy by ID."""
        with self._db.get_db() as db:
            policy = self._zone_policy_repo.get_by_id(db, id=policy_id)
            if not policy:
                raise NotFoundError(f"ZonePolicy '{policy_id}' not found")
            return OutputZonePolicy.from_db(policy)

    def delete_zone_policy(self, policy_id: UUID) -> OutputZonePolicy:
        """Delete a zone policy by ID."""

        with self._db.transaction() as db:
            policy = self._zone_policy_repo.get_by_id(db, id=policy_id)
            if not policy:
                raise NotFoundError(f"ZonePolicy '{policy_id}' not found")
            policy = self._zone_policy_repo.delete(db, entity=policy)
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

    def get_channel_policy(self, policy_id: UUID) -> OutputChannelPolicy:
        """Get a channel policy by ID."""
        with self._db.get_db() as db:
            policy = self._channel_policy_repo.get_by_id(db, id=policy_id)
            if not policy:
                raise NotFoundError(f"ChannelPolicy '{policy_id}' not found")
            return OutputChannelPolicy.from_db(policy)

    def delete_channel_policy(self, policy_id: UUID) -> OutputChannelPolicy:
        """Delete a channel policy by ID."""

        with self._db.transaction() as db:
            policy = self._channel_policy_repo.get_by_id(db, id=policy_id)
            if not policy:
                raise NotFoundError(f"ChannelPolicy '{policy_id}' not found")
            policy = self._channel_policy_repo.delete(db, entity=policy)
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
                core_row = self._ensure_core_row(
                    db,
                    channel_id=channel_id,
                    timeseries_row_id=ts_row_id,
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

    def get_row_policy(self, policy_id: UUID) -> OutputRowPolicy:
        """Get a row policy by ID."""
        with self._db.get_db() as db:
            policy = self._row_policy_repo.get_by_id(db, id=policy_id)
            if not policy:
                raise NotFoundError(f"RowPolicy '{policy_id}' not found")
            return OutputRowPolicy.from_db(policy)

    def delete_row_policy(self, policy_id: UUID) -> OutputRowPolicy:
        """Delete a row policy by ID."""
        with self._db.transaction() as db:
            policy = self._row_policy_repo.get_by_id(db, id=policy_id)
            if not policy:
                raise NotFoundError(f"RowPolicy '{policy_id}' not found")
            policy = self._row_policy_repo.delete(db, entity=policy)
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
            group = self._group_repo.add(db, entity=group)
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

            group = self._group_repo.delete(db, entity=group)
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

    def check_user_has_role(
        self, user_id: UUID, role_id: UUID, indirect: bool = False
    ) -> OutputUserRole | OutputGroupRole | None:
        with self._db.get_db() as db:
            user = self._user_repo.get_by_id(db, id=user_id)
            if user is None:
                return None
            role = self._role_repo.get_by_id(db, id=role_id)
            if role is None:
                return None
            direct_role: RbacUserRoles = self._user_roles_repo.check_link(
                db, filters={RbacLink.USER_ID: user_id, RbacLink.ROLE_ID: role_id}
            )
            # direct = db.query(RbacUserRoles.__table__).filter_by(user_id=user_id, role_id=role_id).first()
            if direct_role is not None:
                return OutputUserRole.from_db(direct_role)
            if not indirect:
                return None

            user_group_ids = self._user_groups_repo.get_group_ids_for_user(db, user_id=user_id)
            all_group_ids = set(user_group_ids)
            for gid in user_group_ids:
                all_group_ids.update(self._get_group_ancestor_ids(db, gid))
            group_role = self._group_roles_repo.get_role_link_for_groups(db, group_ids=all_group_ids, role_id=role_id)

            if not group_role:
                return None
            return OutputGroupRole.from_db(group_role)

    def check_group_has_role(self, group_id: UUID, role_id: UUID, indirect: bool = False) -> OutputGroupRole | None:
        with self._db.get_db() as db:
            group = self._group_repo.get_by_id(db, id=group_id)
            if not group:
                return None
            role = self._role_repo.get_by_id(db, id=role_id)
            if not role:
                return None

            direct_role: RbacGroupRoles = self._group_roles_repo.check_link(
                db, filters={RbacLink.GROUP_ID: group_id, RbacLink.ROLE_ID: role_id}
            )
            if direct_role is not None:
                return OutputGroupRole.from_db(direct_role)
            if not indirect:
                return None

            ancestor_ids = self._get_group_ancestor_ids(db, group_id)
            group_role = self._group_roles_repo.get_role_link_for_groups(db, group_ids=ancestor_ids, role_id=role_id)
            if not group_role:
                return None
            return OutputGroupRole.from_db(group_role)

    def list_role_subjects(self, role_id: UUID, indirect: bool = False) -> OutputRoleSubjects:
        with self._db.get_db() as db:
            user_ids = self._user_roles_repo.get_user_ids_for_role(db, role_id=role_id)
            users: list[OutputUser] = []
            for uid in user_ids:
                u = self._user_repo.get_by_id(db, id=uid)
                if u:
                    users.append(OutputUser.from_db(u))

            group_ids = self._group_roles_repo.get_group_ids_for_role(db, role_id=role_id)
            groups: list[OutputGroup] = []
            for gid in group_ids:
                g = self._group_repo.get_by_id(db, id=gid)
                if g:
                    groups.append(OutputGroup.from_db(g))

            if not indirect:
                return OutputRoleSubjects(users=users, groups=groups)

            indirect_user_ids: set[UUID] = set()
            for gid in group_ids:
                members = self._user_groups_repo.get_user_ids_for_group(db, group_id=gid)
                indirect_user_ids.update(members)
                descendant_ids = self._get_group_descendant_ids(db, gid)
                for desc_id in descendant_ids:
                    members = self._user_groups_repo.get_user_ids_for_group(db, group_id=desc_id)
                    indirect_user_ids.update(members)

            indirect_only = sorted(indirect_user_ids - user_ids)
            indirect_users: list[OutputUser] = []
            for uid in indirect_only:
                u = self._user_repo.get_by_id(db, id=uid)
                if u:
                    indirect_users.append(OutputUser.from_db(u))

            return OutputRoleSubjects(users=users, groups=groups, indirect_users=indirect_users)

    def check_user_in_group(
        self, user_id: UUID, group_id: UUID, indirect: bool = False
    ) -> OutputUserGroupMembership | None:
        with self._db.get_db() as db:
            user = self._user_repo.get_by_id(db, id=user_id)
            if not user:
                return None
            group = self._group_repo.get_by_id(db, id=group_id)
            if not group:
                return None

            direct = self._user_groups_repo.get_membership_link(db, user_id=user_id, group_id=group_id)
            if direct:
                return OutputUserGroupMembership.from_db(direct)
            if not indirect:
                return None

            descendant_ids = self._get_group_descendant_ids(db, group_id)
            for desc_id in descendant_ids:
                link = self._user_groups_repo.get_membership_link(db, user_id=user_id, group_id=desc_id)
                if link:
                    desc_ancestors = self._get_group_ancestor_ids(db, desc_id)
                    target_ancestors = self._get_group_ancestor_ids(db, group_id)
                    chain_ids = (desc_ancestors - target_ancestors) | {group_id}
                    ancestors = []
                    for aid in chain_ids:
                        ag = self._group_repo.get_by_id(db, id=aid)
                        if ag:
                            ancestors.append(OutputGroup.from_db(ag))
                    return OutputUserGroupMembership(
                        user=OutputUser.from_db(user),
                        group=OutputGroup.from_db(self._group_repo.get_by_id(db, id=desc_id)),
                        indirect=True,
                        ancestors=ancestors,
                    )
            return None

    # ----------------------------------------------------------------------------------------------
    # Introspection: subject resolution
    # ----------------------------------------------------------------------------------------------

    def _resolve_user_subject_ids(self, db: Session, user_id: UUID, indirect: bool = False) -> list[UUID]:
        """Return all subject IDs that represent this user (direct + group subjects)."""
        subject_ids: list[UUID] = []

        # User's own subject
        user_subj = db.execute(select(RbacSubject.id).where(RbacSubject.user_id == user_id)).scalars().all()
        subject_ids.extend(user_subj)

        # Direct group subjects
        group_ids = list(self._user_groups_repo.get_group_ids_for_user(db, user_id=user_id))

        if indirect:
            # Expand to ancestor groups
            all_group_ids: set[UUID] = set(group_ids)
            for gid in group_ids:
                group = self._group_repo.get_by_id(db, id=gid)
                if group:
                    ancestors = self.group_hierarchy_service.ancestors(db, group)
                    all_group_ids.update(a.id for a in ancestors)
            group_ids = list(all_group_ids)

        if group_ids:
            group_subjs = db.execute(select(RbacSubject.id).where(RbacSubject.group_id.in_(group_ids))).scalars().all()
            subject_ids.extend(group_subjs)

        return subject_ids

    def _resolve_group_subject_ids(self, db: Session, group_id: UUID, indirect: bool = False) -> list[UUID]:
        """Return all subject IDs that represent this group (direct + ancestor groups)."""
        subject_ids: list[UUID] = []
        group_ids: list[UUID] = [group_id]

        if indirect:
            group = self._group_repo.get_by_id(db, id=group_id)
            if group:
                ancestors = self.group_hierarchy_service.ancestors(db, group)
                group_ids.extend(a.id for a in ancestors)

        if group_ids:
            group_subjs = db.execute(select(RbacSubject.id).where(RbacSubject.group_id.in_(group_ids))).scalars().all()
            subject_ids.extend(group_subjs)

        return subject_ids

    def _get_zone_ancestor_ids(self, db: Session, zone_id: UUID) -> set[UUID]:
        """Return all ancestor zone IDs (excluding the zone itself)."""
        ancestors = db.execute(select(ZoneHierarchy.parent_id).where(ZoneHierarchy.child_id == zone_id)).scalars().all()
        result: set[UUID] = set(ancestors)
        for ancestor_id in ancestors:
            result.update(self._get_zone_ancestor_ids(db, ancestor_id))
        return result

    def _expand_zone_ids_with_ancestors(self, db: Session, zone_ids: set[UUID]) -> set[UUID]:
        """Expand a set of zone IDs to include all their ancestors."""
        expanded = set(zone_ids)
        for zid in zone_ids:
            expanded.update(self._get_zone_ancestor_ids(db, zid))
        return expanded

    def _get_zone_descendant_ids(self, db: Session, zone_id: UUID) -> set[UUID]:
        """Return all descendant zone IDs (excluding the zone itself)."""
        descendants = (
            db.execute(select(ZoneHierarchy.child_id).where(ZoneHierarchy.parent_id == zone_id)).scalars().all()
        )
        result: set[UUID] = set(descendants)
        for desc_id in descendants:
            result.update(self._get_zone_descendant_ids(db, desc_id))
        return result

    # ----------------------------------------------------------------------------------------------
    # Introspection: permissions
    # ----------------------------------------------------------------------------------------------

    def get_user_permissions(self, user_id: UUID) -> OutputUserPermissions:
        """Return the full permission landscape for a user."""

        with self._db.get_db() as db:
            user = self._user_repo.get_by_id(db, id=user_id)
            if not user:
                return OutputUserPermissions()

            roles: list[OutputUserRole] = []

            # Direct user roles
            user_roles = self._user_roles_repo.list_roles_for_user(db, user_id=user_id)
            roles.extend([OutputUserRole.from_db(u) for u in user_roles])

            # Group roles (indirect via group membership + hierarchy)
            indirect_roles: list[OutputGroupRole] = []
            user_groups: list[RbacUserGroups] = self._user_groups_repo.list_groups_for_user(db, user_id=user_id)
            ancestor_ids: set[UUID] = set()
            for ug in user_groups:
                ancestor_ids.update(self.group_hierarchy_service.ancestors_ids(db, ug.group))
            all_group_ids = {ug.group_id for ug in user_groups} | ancestor_ids
            if all_group_ids:
                group_roles = self._group_roles_repo.list_roles_for_groups(db, group_ids=all_group_ids)
                indirect_roles.extend([OutputGroupRole.from_db(gr) for gr in group_roles])

            # Policies via subject resolution
            subject_ids = self._resolve_user_subject_ids(db, user_id, indirect=False)
            zone_policies: list[OutputZonePolicy] = []
            channel_policies: list[OutputChannelPolicy] = []
            row_policies: list[OutputRowPolicy] = []
            if subject_ids:
                zone_policies = [
                    OutputZonePolicy.from_db(p)
                    for p in self._zone_policy_repo.get_policies_for_subjects(db, subject_ids=subject_ids)
                ]
                channel_policies = [
                    OutputChannelPolicy.from_db(p)
                    for p in self._channel_policy_repo.get_policies_for_subjects(db, subject_ids=subject_ids)
                ]
                row_policies = [
                    OutputRowPolicy.from_db(p)
                    for p in self._row_policy_repo.get_policies_for_subjects(db, subject_ids=subject_ids)
                ]

            return OutputUserPermissions(
                user=OutputUser.from_db(user),
                roles=roles,
                direct_roles=roles,
                indirect_roles=indirect_roles,
                group_roles=indirect_roles,
                zone_policies=zone_policies,
                channel_policies=channel_policies,
                row_policies=row_policies,
            )

    def get_group_permissions(self, group_id: UUID) -> OutputGroupPermissions:
        """Return the full permission landscape for a group."""

        with self._db.get_db() as db:
            group = self._group_repo.get_by_id(db, id=group_id)
            if not group:
                return OutputGroupPermissions()

            roles: list[OutputGroupRole] = []

            # Direct group roles
            direct_roles = self._group_roles_repo.list_roles_for_group(db, group_id=group_id)
            roles.extend([OutputGroupRole.from_db(gr) for gr in direct_roles])

            # Indirect group roles (indirect via group hierarchy)
            indirect_roles: list[OutputGroupRole] = []
            ancestor_ids = self.group_hierarchy_service.ancestors_ids(db, group)
            if ancestor_ids:
                indirect_roles = [
                    OutputGroupRole.from_db(gr)
                    for gr in self._group_roles_repo.list_roles_for_groups(db, group_ids=ancestor_ids)
                ]

            # Policies via subject resolution
            subject_ids = self._resolve_group_subject_ids(db, group_id, indirect=False)
            zone_policies: list[OutputZonePolicy] = []
            channel_policies: list[OutputChannelPolicy] = []
            row_policies: list[OutputRowPolicy] = []
            if subject_ids:
                zone_policies = [
                    OutputZonePolicy.from_db(p)
                    for p in self._zone_policy_repo.get_policies_for_subjects(db, subject_ids=subject_ids)
                ]
                channel_policies = [
                    OutputChannelPolicy.from_db(p)
                    for p in self._channel_policy_repo.get_policies_for_subjects(db, subject_ids=subject_ids)
                ]
                row_policies = [
                    OutputRowPolicy.from_db(p)
                    for p in self._row_policy_repo.get_policies_for_subjects(db, subject_ids=subject_ids)
                ]

            return OutputGroupPermissions(
                group=OutputGroup.from_db(group),
                roles=roles,
                direct_roles=roles,
                indirect_roles=indirect_roles,
                group_roles=indirect_roles,
                zone_policies=zone_policies,
                channel_policies=channel_policies,
                row_policies=row_policies,
            )

    # ----------------------------------------------------------------------------------------------
    # Introspection: resource access
    # ----------------------------------------------------------------------------------------------

    def _build_resource_access(self, policy, resource, parent=None):
        """Build a ResourceAccess from a policy and its resource."""

        res_ref = OutputSchema(id=resource.id, name=resource.name)
        parent_ref = OutputSchema(id=parent.id, name=parent.name) if parent else None

        if isinstance(policy, ZonePolicy):
            out_policy = OutputZonePolicy.from_db(policy)
        elif isinstance(policy, ChannelPolicy):
            out_policy = OutputChannelPolicy.from_db(policy)
        elif isinstance(policy, RowPolicy):
            out_policy = OutputRowPolicy.from_db(policy)
        else:
            out_policy = policy

        return ResourceAccess(resource=res_ref, parent=parent_ref, policy=out_policy)

    def get_user_zones(self, user_id: UUID, indirect: bool = False) -> list:
        """Return zones the user has access to via policies."""
        with self._db.get_db() as db:
            subject_ids = self._resolve_user_subject_ids(db, user_id, indirect=indirect)
            if not subject_ids:
                return []

            policies = self._zone_policy_repo.get_policies_for_subjects(db, subject_ids=subject_ids)
            zone_map: dict[UUID, list] = {}
            for p in policies:
                zone_id = p.access_profile.zone_id
                zone_map.setdefault(zone_id, []).append(p)

            zone_ids = set(zone_map.keys())
            if indirect:
                zone_ids = self._expand_zone_ids_with_ancestors(db, zone_ids)

            result = []
            for zid in zone_ids:
                zone = self._zone_repo.get_by_id(db, id=zid)
                if not zone:
                    continue
                for p in zone_map.get(zid, []):
                    result.append(self._build_resource_access(p, zone))
            return result

    def _build_direct_channel_map(self, db, subject_ids: list) -> dict[UUID, list]:
        """Map channel_id → list[policy] from direct channel policies."""
        ch_map: dict[UUID, list] = {}
        for p in self._channel_policy_repo.get_policies_for_subjects(db, subject_ids=subject_ids):
            ch_map.setdefault(p.access_profile.channel_id, []).append(p)
        return ch_map

    def _expand_zone_ids_for_channels(self, db, zone_policies: list, indirect: bool) -> set[UUID]:
        """Return zone IDs (optionally expanded with ancestors and descendants)."""
        zone_ids = {p.access_profile.zone_id for p in zone_policies}
        if indirect:
            zone_ids = self._expand_zone_ids_with_ancestors(db, zone_ids)
        all_zone_ids = set(zone_ids)
        if indirect:
            for zid in list(zone_ids):
                all_zone_ids.update(self._get_zone_descendant_ids(db, zid))
        return all_zone_ids

    def _attach_zone_channels_to_map(
        self, db, zone_policies: list, all_zone_ids: set[UUID], ch_map: dict[UUID, list]
    ) -> None:
        """Populate ch_map with channels found in the given zones, attaching zone policies."""
        for zid in all_zone_ids:
            for ch in self._channel_repo.get_by_zone(db, zone_id=zid):
                ch_map.setdefault(ch.id, [])
                for zp in zone_policies:
                    if zp.access_profile.zone_id == zid:
                        ch_map[ch.id].append(zp)

    def _build_channel_access_results(self, db, ch_map: dict[UUID, list]) -> list:
        """Resolve channels and build the final resource-access list."""
        result = []
        for ch_id, policies in ch_map.items():
            channel = self._channel_repo.get_by_id(db, id=ch_id)
            if not channel:
                continue
            parent_zone = None
            if channel.zone_id:
                parent_zone = self._zone_repo.get_by_id(db, id=channel.zone_id)
            for p in policies:
                result.append(self._build_resource_access(p, channel, parent=parent_zone))
        return result

    def get_user_channels(self, user_id: UUID, indirect: bool = False) -> list:
        """Return channels the user has access to via policies."""
        with self._db.get_db() as db:
            subject_ids = self._resolve_user_subject_ids(db, user_id, indirect=indirect)
            if not subject_ids:
                return []

            ch_map = self._build_direct_channel_map(db, subject_ids)
            zone_policies = self._zone_policy_repo.get_policies_for_subjects(db, subject_ids=subject_ids)
            all_zone_ids = self._expand_zone_ids_for_channels(db, zone_policies, indirect)
            self._attach_zone_channels_to_map(db, zone_policies, all_zone_ids, ch_map)
            return self._build_channel_access_results(db, ch_map)

    def get_user_rows(self, user_id: UUID, indirect: bool = False) -> list:
        """Return rows the user has access to via policies."""
        with self._db.get_db() as db:
            subject_ids = self._resolve_user_subject_ids(db, user_id, indirect=indirect)
            if not subject_ids:
                return []

            policies = self._row_policy_repo.get_policies_for_subjects(db, subject_ids=subject_ids)
            result = []
            for p in policies:
                row_id = p.access_profile.row_id
                row = self._row_repo.get_by_id(db, id=row_id)
                if not row:
                    continue
                channel = self._channel_repo.get_by_id(db, id=row.channel_id) if row.channel_id else None
                result.append(self._build_resource_access(p, row, parent=channel))
            return result

    def get_user_resources(self, user_id: UUID, indirect: bool = False) -> dict:
        """Return all resources (zones, channels, rows) the user has access to."""
        return {
            "zones": self.get_user_zones(user_id, indirect=indirect),
            "channels": self.get_user_channels(user_id, indirect=indirect),
            "rows": self.get_user_rows(user_id, indirect=indirect),
        }

    def get_group_zones(self, group_id: UUID, indirect: bool = False) -> list:
        """Return zones the group has access to via policies."""
        with self._db.get_db() as db:
            subject_ids = self._resolve_group_subject_ids(db, group_id, indirect=indirect)
            if not subject_ids:
                return []

            policies = self._zone_policy_repo.get_policies_for_subjects(db, subject_ids=subject_ids)
            zone_map: dict[UUID, list] = {}
            for p in policies:
                zone_id = p.access_profile.zone_id
                zone_map.setdefault(zone_id, []).append(p)

            zone_ids = set(zone_map.keys())
            if indirect:
                zone_ids = self._expand_zone_ids_with_ancestors(db, zone_ids)

            result = []
            for zid in zone_ids:
                zone = self._zone_repo.get_by_id(db, id=zid)
                if not zone:
                    continue
                for p in zone_map.get(zid, []):
                    result.append(self._build_resource_access(p, zone))
            return result

    def get_group_channels(self, group_id: UUID, indirect: bool = False) -> list:
        """Return channels the group has access to via policies."""
        with self._db.get_db() as db:
            subject_ids = self._resolve_group_subject_ids(db, group_id, indirect=indirect)
            if not subject_ids:
                return []

            ch_map = self._build_direct_channel_map(db, subject_ids)
            zone_policies = self._zone_policy_repo.get_policies_for_subjects(db, subject_ids=subject_ids)
            all_zone_ids = self._expand_zone_ids_for_channels(db, zone_policies, indirect)
            self._attach_zone_channels_to_map(db, zone_policies, all_zone_ids, ch_map)
            return self._build_channel_access_results(db, ch_map)

    def get_group_rows(self, group_id: UUID, indirect: bool = False) -> list:
        """Return rows the group has access to via policies."""
        with self._db.get_db() as db:
            subject_ids = self._resolve_group_subject_ids(db, group_id, indirect=indirect)
            if not subject_ids:
                return []

            policies = self._row_policy_repo.get_policies_for_subjects(db, subject_ids=subject_ids)
            result = []
            for p in policies:
                row_id = p.access_profile.row_id
                row = self._row_repo.get_by_id(db, id=row_id)
                if not row:
                    continue
                channel = self._channel_repo.get_by_id(db, id=row.channel_id) if row.channel_id else None
                result.append(self._build_resource_access(p, row, parent=channel))
            return result

    def get_group_resources(self, group_id: UUID, indirect: bool = False) -> dict:
        """Return all resources (zones, channels, rows) the group has access to."""
        return {
            "zones": self.get_group_zones(group_id, indirect=indirect),
            "channels": self.get_group_channels(group_id, indirect=indirect),
            "rows": self.get_group_rows(group_id, indirect=indirect),
        }

    # ----------------------------------------------------------------------------------------------
    # Introspection: resource policy/access-profile lists
    # ----------------------------------------------------------------------------------------------

    def get_zone_policies(self, zone_id: UUID) -> list[OutputZonePolicy]:
        """Return all policies on a zone."""
        with self._db.get_db() as db:
            policies = self._zone_policy_repo.get_policies_for_zone(db, zone_id=zone_id)
            return [OutputZonePolicy.from_db(p) for p in policies]

    def get_zone_access_profiles(self, zone_id: UUID) -> list[OutputZoneAccessProfile]:
        """Return all access profiles on a zone."""
        with self._db.get_db() as db:
            stmt = select(ZoneAccessProfile).where(ZoneAccessProfile.zone_id == zone_id)
            profiles = list(db.execute(stmt).scalars().all())
            return [OutputZoneAccessProfile.from_db(p) for p in profiles]

    def get_channel_policies(self, channel_id: UUID) -> list[OutputChannelPolicy]:
        """Return all policies on a channel."""
        with self._db.get_db() as db:
            policies = self._channel_policy_repo.get_policies_for_channel(db, channel_id=channel_id)
            return [OutputChannelPolicy.from_db(p) for p in policies]

    def get_channel_access_profiles(self, channel_id: UUID) -> list[OutputChannelAccessProfile]:
        """Return all access profiles on a channel."""
        with self._db.get_db() as db:
            stmt = select(ChannelAccessProfile).where(ChannelAccessProfile.channel_id == channel_id)
            profiles = list(db.execute(stmt).scalars().all())
            return [OutputChannelAccessProfile.from_db(p) for p in profiles]

    def get_row_policies(self, row_id: UUID) -> list[OutputRowPolicy]:
        """Return all policies on a row."""
        with self._db.get_db() as db:
            policies = self._row_policy_repo.get_policies_for_row(db, row_id=row_id)
            return [OutputRowPolicy.from_db(p) for p in policies]

    def get_row_access_profiles(self, row_id: UUID) -> list[OutputRowAccessProfile]:
        """Return all access profiles on a row."""
        with self._db.get_db() as db:
            stmt = select(RowAccessProfile).where(RowAccessProfile.row_id == row_id)
            profiles = list(db.execute(stmt).scalars().all())
            return [OutputRowAccessProfile.from_db(p) for p in profiles]
