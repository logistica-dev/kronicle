# kronicle/services/rbac_service.py
import functools
from collections.abc import Sequence
from uuid import UUID

from sqlalchemy import and_, cast, delete, func, select
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.session import Session

from kronicle.db.core.links.zone_hierarchy import ZoneHierarchy
from kronicle.db.core.models.core_channel import CoreChannel
from kronicle.db.core.models.core_zone import CoreZone
from kronicle.db.rbac.links.group_hierarchy import RbacGroupHierarchy
from kronicle.db.rbac.links.group_roles import RbacGroupRoles
from kronicle.db.rbac.links.user_groups import RbacUserGroups
from kronicle.db.rbac.links.user_roles import RbacUserRoles
from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_role import RbacRole
from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.db.rbac.rbac_db_session import RbacDbSession
from kronicle.errors.error_types import BadRequestError, ConflictError, NotFoundError, UnauthorizedError
from kronicle.repo.core.core_channel_repo import CoreChannelRepository
from kronicle.repo.core.core_zone_repo import CoreZoneRepository
from kronicle.repo.hierarchy.hierarchy_engine import HierarchyEngine
from kronicle.repo.hierarchy.hierarchy_service import HierarchyService
from kronicle.repo.hierarchy.zone_hierarchy_repo import ZoneHierarchyRepository
from kronicle.repo.rbac.entities.rbac_group_repo import RbacGroupRepository
from kronicle.repo.rbac.entities.rbac_role_repo import RbacRoleRepository
from kronicle.repo.rbac.entities.rbac_user_repo import RbacUserRepository
from kronicle.repo.rbac.links.rbac_user_group_repo import RbacUserGroupRepository
from kronicle.schemas.core.safe_core_channel_schemas import OutputCoreChannel
from kronicle.schemas.core.safe_zone_schemas import OutputZone
from kronicle.schemas.permissions.permission import Permission
from kronicle.schemas.rbac.input_user_schemas import InputUserLogin
from kronicle.schemas.rbac.safe_group_schemas import OutputGroup
from kronicle.schemas.rbac.safe_role_schemas import OutputRole
from kronicle.schemas.rbac.safe_user_schemas import OutputUser, ProcessedUser
from kronicle.utils.dev_logs import log_d, log_i, log_w
from kronicle.utils.str_utils import uuid_to_str


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

mod = "rbacs"


class RbacService:
    def __init__(
        self,
        rbac_db_session: RbacDbSession,
    ):
        self._db = rbac_db_session
        self._user_repo = RbacUserRepository()
        self._group_repo = RbacGroupRepository()
        self._role_repo = RbacRoleRepository()
        self._user_groups_repo = RbacUserGroupRepository()
        self._channel_repo = CoreChannelRepository()
        self._zone_repo = CoreZoneRepository()
        self._zone_hierarchy_repo = ZoneHierarchyRepository()

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

        zone_engine = HierarchyEngine(
            parents_of=lambda g: g.parent,
            children_of=lambda g: g.children,
        )

        self.zone_hierarchy_service = HierarchyService(
            engine=zone_engine,
            add_edge=ZoneHierarchy.add,
            remove_edge=ZoneHierarchy.remove,
            max_parents=1,
        )

    # ----------------------------------------------------------------------------------------------
    # Read-only: fetch user info
    # ----------------------------------------------------------------------------------------------
    def _fetch_user_by_login(self, login_input: InputUserLogin) -> RbacUser:
        with self._db.get_db() as db:  # read-only
            # TODO: remove this!
            users = self._user_repo.fetch_all(db, include_superusers=True)
            log_d(mod, "fetch_user_info", "users:", users)
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
        return OutputUser.from_db_user(db_user)

    def fetch_user_by_email(self, email: str) -> OutputUser | None:
        with self._db.get_db() as db:  # read-only
            db_user = self._user_repo.get_by_email(db, email=email)
        return OutputUser.from_db_user(db_user) if db_user else None

    def fetch_user_by_name(self, name: str) -> OutputUser | None:
        with self._db.get_db() as db:  # read-only
            db_user = self._user_repo.get_by_name(db, name=name)
        return OutputUser.from_db_user(db_user) if db_user else None

    def fetch_user_by_id(self, id: UUID) -> OutputUser | None:
        with self._db.get_db() as db:  # read-only
            db_user = self._user_repo.get_by_id(db, id=id)
        return OutputUser.from_db_user(db_user) if db_user else None

    def fetch_user_by_external_id(self, orcid: str) -> OutputUser | None:
        with self._db.get_db() as db:  # read-only
            db_user = self._user_repo.get_by_external_id(db, external_id=orcid)
        return OutputUser.from_db_user(db_user) if db_user else None

    def list_users(self, *, include_inactive: bool = False) -> list[OutputUser]:
        here = mod + ".list_users"
        log_d(here, "include_inactive", include_inactive)
        with self._db.get_db() as db:  # read-only
            users = self._user_repo.fetch_all(db, include_inactive=include_inactive)
        return [OutputUser.from_db_user(u) for u in users]

    # ----------------------------------------------------------------------------------------------
    # Write: create user
    # ----------------------------------------------------------------------------------------------
    def create_user(self, user: ProcessedUser) -> OutputUser:
        here = "create_usr"
        log_d(here, user.email)
        if not user.password_hash:
            raise BadRequestError("Input user password should be provided")
        rbac_user = user.to_db_user()

        with self._db.transaction() as db:
            existing = self._user_repo.get_by_email(db=db, email=rbac_user.email)
            if existing:
                raise UnauthorizedError("User already exists")
            log_d(here, rbac_user.name)
            db_user = self._user_repo.create_user(db=db, user=rbac_user)
        out_user = OutputUser.from_db_user(db_user)
        return out_user

    def patch_user(self, user: ProcessedUser) -> OutputUser:
        here = "patch_user"
        log_i(here, user.email)
        with self._db.transaction() as db:
            db_user: RbacUser = self._user_repo.get_by_email(db=db, email=user.email)
            if not db_user:
                raise UnauthorizedError("User doesn't exists")
            updated = False
            if user.name is not None:
                db_user.name = user.name
                updated = True
            if user.full_name is not None:
                db_user.full_name = user.full_name
                updated = True
            if user.orcid is not None:
                db_user.external_id = user.orcid
                updated = True
            if updated:
                try:
                    db.commit()
                except IntegrityError as e:
                    log_w(here, "IntegrityError", e)
                    raise UnauthorizedError("Attempting to patch with existing values") from e

            db.refresh(db_user)
        return OutputUser.from_db_user(db_user)

    def update_password_hash(self, user_id: UUID, new_hash: str) -> None:
        with self._db.transaction() as db:
            self._user_repo.update_password_hash(db, user_id=user_id, new_hash=new_hash)

    # ----------------------------------------------------------------------------------------------
    # Write: delete (deactivate/remove) user
    # ----------------------------------------------------------------------------------------------

    def _deactivate_user(self, db: Session, db_user: RbacUser | None):
        here = "_deact_usr"
        if not db_user:
            raise UnauthorizedError("User doesn't exists")
        log_i(here, db_user.snapshot)
        db_user.is_active = False
        db.commit()
        db.refresh(db_user)
        return OutputUser.from_db_user(db_user)

    def _delete_user(self, db: Session, db_user: RbacUser | None):
        here = "_del_usr"
        if not db_user:
            raise UnauthorizedError("User doesn't exists")
        log_w(here, db_user.snapshot)

        # Clear role and group assignments explicitly — even with ondelete=CASCADE,
        # SQLAlchemy's unitofwork processor tries to NULL PK columns before
        # the DB-level cascade, causing an AssertionError.
        db.execute(delete(RbacUserRoles.__table__).where(RbacUserRoles.user_id == db_user.id))
        db.execute(delete(RbacUserGroups.__table__).where(RbacUserGroups.user_id == db_user.id))

        deleted_user = self._user_repo.delete_user(db, user=db_user)
        db.commit()
        return OutputUser.from_db_user(deleted_user)

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
    def user_has_permission(self, user_id: UUID, permission: str | Permission) -> bool:
        perm_str = str(permission) if isinstance(permission, Permission) else permission
        Permission.parse(perm_str)
        with self._db.get_db() as db:
            perm_jsonb = cast(func.json_build_array(perm_str), JSONB)
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
            if not group_ids:
                return False
            has_group = db.execute(
                select(RbacGroupRoles.role_id)
                .join(RbacRole, RbacRole.id == RbacGroupRoles.role_id)
                .where(RbacGroupRoles.group_id.in_(group_ids))
                .where(RbacRole.permissions.op("@>")(perm_jsonb))
            ).first()
            return has_group is not None

    # ----------------------------------------------------------------------------------------------
    # User ↔ Role assignment
    # ----------------------------------------------------------------------------------------------

    def assign_role_to_user(self, user_id: UUID, role_id: UUID) -> None:
        with self._db.transaction() as db:
            stmt = insert(RbacUserRoles.__table__).values(user_id=user_id, role_id=role_id).on_conflict_do_nothing()
            db.execute(stmt)

    def remove_role_from_user(self, user_id: UUID, role_id: UUID) -> None:
        with self._db.transaction() as db:
            stmt = delete(RbacUserRoles.__table__).where(
                and_(
                    RbacUserRoles.user_id == user_id,
                    RbacUserRoles.role_id == role_id,
                )
            )
            db.execute(stmt)

    # ----------------------------------------------------------------------------------------------
    # Group ↔ Role assignment
    # ----------------------------------------------------------------------------------------------

    def assign_role_to_group(self, group_id: UUID, role_id: UUID) -> None:
        with self._db.transaction() as db:
            stmt = insert(RbacGroupRoles.__table__).values(group_id=group_id, role_id=role_id).on_conflict_do_nothing()
            db.execute(stmt)

    def remove_role_from_group(self, group_id: UUID, role_id: UUID) -> None:
        with self._db.transaction() as db:
            stmt = delete(RbacGroupRoles.__table__).where(
                and_(
                    RbacGroupRoles.group_id == group_id,
                    RbacGroupRoles.role_id == role_id,
                )
            )
            db.execute(stmt)

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
        from kronicle.db.rbac.models.rbac_role import RbacRole

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
        return OutputRole.from_db_role(role)

    def get_roles(self) -> list[OutputRole]:
        with self._db.get_db() as db:
            roles = self._role_repo.fetch_all(db)
        return [OutputRole.from_db_role(r) for r in roles]

    def get_role(self, role_id: UUID) -> OutputRole | None:
        with self._db.get_db() as db:
            role = self._role_repo.get_by_id(db, id=role_id)
        return OutputRole.from_db_role(role) if role else None

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
        return OutputRole.from_db_role(role)

    def delete_role(self, role_id: UUID) -> OutputRole | None:
        here = "delete_role"
        log_w(here, role_id)
        with self._db.transaction() as db:
            role = self._role_repo.get_by_id(db, id=role_id)
            if not role:
                raise NotFoundError(f"Role '{role_id}' not found")

            # Check for dependent assignments before deletion
            conflicts = []
            user_count = db.execute(
                select(func.count(RbacUserRoles.role_id)).where(RbacUserRoles.role_id == role_id)
            ).scalar()
            if user_count:
                conflicts.append(f"assigned to {user_count} user(s)")

            group_count = db.execute(
                select(func.count(RbacGroupRoles.role_id)).where(RbacGroupRoles.role_id == role_id)
            ).scalar()
            if group_count:
                conflicts.append(f"assigned to {group_count} group(s)")

            if conflicts:
                raise ConflictError(
                    f"Role '{role.name}' cannot be deleted: {'; '.join(conflicts)}. " "Remove these assignments first."
                )

            db.delete(role)
            db.flush()
        return OutputRole.from_db_role(role)

    # ----------------------------------------------------------------------------------------------
    # Policies
    # ----------------------------------------------------------------------------------------------
    def _ensure_subject(self, db: Session, subject_id: UUID) -> None:
        """Ensure an RbacSubject entry exists for a user or group ID."""
        from kronicle.db.rbac.models.rbac_subject import RbacSubject

        existing = db.get(RbacSubject, subject_id)
        if existing:
            return

        # Determine if it's a user or group
        user = self._user_repo.get_by_id(db, id=subject_id, include_inactive=True)
        if user:
            subject = RbacSubject(id=subject_id, type="user")
            db.add(subject)
            db.flush()
            return

        group = self._group_repo.get_by_id(db, id=subject_id)
        if group:
            subject = RbacSubject(id=subject_id, type="group")
            db.add(subject)
            db.flush()
            return

        raise NotFoundError(f"Subject '{subject_id}' not found as user or group")

    def _ensure_zone_access_profile(self, db: Session, role_id: UUID, zone_id: UUID) -> UUID:
        """Find or create a ZoneAccessProfile for the given role and zone. Returns its ID."""
        from kronicle.db.rbac.links.rbac_access_profile import ZoneAccessProfile

        existing = db.query(ZoneAccessProfile).filter_by(role_id=role_id, zone_id=zone_id).first()
        if existing:
            return existing.id

        # Verify zone exists
        zone = self._zone_repo.get_by_id(db, id=zone_id)
        if not zone:
            raise NotFoundError(f"Zone '{zone_id}' not found")

        profile = ZoneAccessProfile(role_id=role_id, zone_id=zone_id)
        db.add(profile)
        db.flush()
        return profile.id

    def _ensure_channel_access_profile(self, db: Session, role_id: UUID, channel_id: UUID) -> UUID:
        """Find or create a ChannelAccessProfile for the given role and channel. Returns its ID."""
        from kronicle.db.rbac.links.rbac_access_profile import ChannelAccessProfile

        existing = db.query(ChannelAccessProfile).filter_by(role_id=role_id, channel_id=channel_id).first()
        if existing:
            return existing.id

        # Verify channel exists
        channel = self._channel_repo.get_by_id(db, id=channel_id)
        if not channel:
            raise NotFoundError(f"CoreChannel '{channel_id}' not found")

        profile = ChannelAccessProfile(role_id=role_id, channel_id=channel_id)
        db.add(profile)
        db.flush()
        return profile.id

    def create_zone_policy(self, subject_id: UUID, role_id: UUID, zone_id: UUID) -> dict:
        """Assign a role to a subject (user or group) for a specific zone."""
        here = "create_zone_policy"
        log_d(here, subject_id, role_id, zone_id)

        from kronicle.db.rbac.models.rbac_policy import ZonePolicy
        from kronicle.db.rbac.models.rbac_role import RbacRole

        with self._db.transaction() as db:
            # Verify role exists
            role = db.get(RbacRole, role_id)
            if not role:
                raise NotFoundError(f"Role '{role_id}' not found")

            # Ensure subject exists in rbac.subjects
            self._ensure_subject(db, subject_id)

            # Find or create the ZoneAccessProfile
            access_profile_id = self._ensure_zone_access_profile(db, role_id, zone_id)

            # Create the policy
            policy = ZonePolicy(
                subject_id=subject_id,
                access_profile_id=access_profile_id,
            )
            db.add(policy)
            db.flush()
            db.refresh(policy)

        return {
            "id": uuid_to_str(policy.id),
            "subject_id": uuid_to_str(subject_id),
            "role_id": uuid_to_str(role_id),
            "role_name": role.name,
            "zone_id": uuid_to_str(zone_id),
            "is_delegation": policy.is_delegation,
        }

    def create_channel_policy(self, subject_id: UUID, role_id: UUID, channel_id: UUID) -> dict:
        """Assign a role to a subject (user or group) for a specific channel."""
        here = "create_channel_policy"
        log_d(here, subject_id, role_id, channel_id)

        from kronicle.db.rbac.models.rbac_policy import ChannelPolicy
        from kronicle.db.rbac.models.rbac_role import RbacRole

        with self._db.transaction() as db:
            # Verify role exists
            role = db.get(RbacRole, role_id)
            if not role:
                raise NotFoundError(f"Role '{role_id}' not found")

            # Ensure subject exists
            self._ensure_subject(db, subject_id)

            # Find or create the ChannelAccessProfile
            access_profile_id = self._ensure_channel_access_profile(db, role_id, channel_id)

            # Create the policy
            policy = ChannelPolicy(
                subject_id=subject_id,
                access_profile_id=access_profile_id,
            )
            db.add(policy)
            db.flush()
            db.refresh(policy)

        return {
            "id": uuid_to_str(policy.id),
            "subject_id": uuid_to_str(subject_id),
            "role_id": uuid_to_str(role_id),
            "role_name": role.name,
            "channel_id": uuid_to_str(channel_id),
            "is_delegation": policy.is_delegation,
        }

    def list_zone_policies(self, zone_id: UUID) -> list[dict]:
        """List all policies for a zone."""
        from kronicle.db.rbac.links.rbac_access_profile import ZoneAccessProfile
        from kronicle.db.rbac.models.rbac_policy import ZonePolicy
        from kronicle.db.rbac.models.rbac_role import RbacRole

        with self._db.get_db() as db:
            policies = (
                db.query(ZonePolicy)
                .join(ZoneAccessProfile, ZonePolicy.access_profile_id == ZoneAccessProfile.id)
                .join(RbacRole, ZoneAccessProfile.role_id == RbacRole.id)
                .filter(ZoneAccessProfile.zone_id == zone_id)
                .all()
            )
            results = []
            for p in policies:
                profile = db.query(ZoneAccessProfile).filter_by(id=p.access_profile_id).first()
                role = db.get(RbacRole, profile.role_id) if profile else None
                results.append(
                    {
                        "id": uuid_to_str(p.id),
                        "subject_id": uuid_to_str(p.subject_id),
                        "role_id": uuid_to_str(profile.role_id) if profile else None,
                        "role_name": role.name if role else None,
                        "zone_id": uuid_to_str(zone_id),
                        "is_delegation": p.is_delegation,
                    }
                )
            return results

    def list_channel_policies(self, channel_id: UUID) -> list[dict]:
        """List all policies for a channel."""
        from kronicle.db.rbac.links.rbac_access_profile import ChannelAccessProfile
        from kronicle.db.rbac.models.rbac_policy import ChannelPolicy
        from kronicle.db.rbac.models.rbac_role import RbacRole

        with self._db.get_db() as db:
            policies = (
                db.query(ChannelPolicy)
                .join(ChannelAccessProfile, ChannelPolicy.access_profile_id == ChannelAccessProfile.id)
                .join(RbacRole, ChannelAccessProfile.role_id == RbacRole.id)
                .filter(ChannelAccessProfile.channel_id == channel_id)
                .all()
            )
            results = []
            for p in policies:
                profile = db.query(ChannelAccessProfile).filter_by(id=p.access_profile_id).first()
                role = db.get(RbacRole, profile.role_id) if profile else None
                results.append(
                    {
                        "id": uuid_to_str(p.id),
                        "subject_id": uuid_to_str(p.subject_id),
                        "role_id": uuid_to_str(profile.role_id) if profile else None,
                        "role_name": role.name if role else None,
                        "channel_id": uuid_to_str(channel_id),
                        "is_delegation": p.is_delegation,
                    }
                )
            return results

    def delete_zone_policy(self, policy_id: UUID) -> None:
        """Delete a zone policy by ID."""
        from kronicle.db.rbac.models.rbac_policy import ZonePolicy

        with self._db.transaction() as db:
            policy = db.get(ZonePolicy, policy_id)
            if not policy:
                raise NotFoundError(f"ZonePolicy '{policy_id}' not found")
            db.delete(policy)
            db.flush()

    def delete_channel_policy(self, policy_id: UUID) -> None:
        """Delete a channel policy by ID."""
        from kronicle.db.rbac.models.rbac_policy import ChannelPolicy

        with self._db.transaction() as db:
            policy = db.get(ChannelPolicy, policy_id)
            if not policy:
                raise NotFoundError(f"ChannelPolicy '{policy_id}' not found")
            db.delete(policy)
            db.flush()

    # ----------------------------------------------------------------------------------------------
    # Core: Zones
    # ----------------------------------------------------------------------------------------------
    def get_zones(self) -> list[CoreZone]:
        with self._db.get_db() as db:
            return list(self._zone_repo.fetch_all(db))

    def create_zone(self, name: str, details: dict | None = None) -> OutputZone:
        here = "create_zone"
        log_d(here, name)
        zone = CoreZone(name=name, details=details or {})
        with self._db.transaction() as db:
            existing = self._zone_repo.get_by_name(db, name=name)
            if existing:
                raise BadRequestError(f"Zone '{name}' already exists")
            db.add(zone)
            db.flush()
        return OutputZone.from_db_zone(zone)

    def get_zone(self, zone_id: UUID) -> OutputZone | None:
        with self._db.get_db() as db:
            zone = self._zone_repo.get_by_id(db, id=zone_id)
        return OutputZone.from_db_zone(zone) if zone else None

    def delete_zone(self, zone_id: UUID) -> OutputZone | None:
        here = "delete_zone"
        log_w(here, zone_id)
        with self._db.transaction() as db:
            zone = self._zone_repo.get_by_id(db, id=zone_id)
            if not zone:
                raise NotFoundError(f"Zone '{zone_id}' not found")
            db.delete(zone)
            db.flush()
        return OutputZone.from_db_zone(zone)

    def patch_zone(self, zone_id: UUID, name: str | None = None, details: dict | None = None) -> OutputZone:
        here = "patch_zone"
        log_d(here, zone_id)
        with self._db.transaction() as db:
            zone = self._zone_repo.get_by_id(db, id=zone_id)
            if not zone:
                raise NotFoundError(f"Zone '{zone_id}' not found")
            if name is not None:
                zone.name = name
            if details is not None:
                zone.details = details
            db.flush()
            db.refresh(zone)
        return OutputZone.from_db_zone(zone)

    # ----------------------------------------------------------------------------------------------
    # Sync: Channels
    # ----------------------------------------------------------------------------------------------
    def list_core_channel_ids(self) -> set[UUID]:
        with self._db.get_db() as db:
            channels = self._channel_repo.fetch_all(db)
        return {c.id for c in channels}

    def sync_core_channels(self, channel_ids: Sequence[UUID], default_zone_id: UUID | None = None) -> list[UUID]:
        here = "sync_core_channels"
        existing = self.list_core_channel_ids()
        missing = [cid for cid in channel_ids if cid not in existing]
        if not missing:
            log_d(here, "All channels already synced")
            return []

        created: list[UUID] = []
        with self._db.transaction() as db:
            for cid in missing:
                core_channel = CoreChannel(id=cid, name=str(cid), zone_id=default_zone_id)
                db.add(core_channel)
                created.append(cid)
                log_d(here, f"Created CoreChannel for {cid}")
            db.flush()
        log_i(here, f"Created {len(created)} CoreChannels")
        return created

    def ensure_default_zone(self, name: str = "default") -> CoreZone:
        with self._db.get_db() as db:
            existing = self._zone_repo.get_by_name(db, name=name)
            if existing:
                return existing
        zone = CoreZone(name=name)
        with self._db.transaction() as db:
            db.add(zone)
            db.flush()
        log_i("ensure_default_zone", f"Created default zone '{name}'")
        return zone

    # ----------------------------------------------------------------------------------------------
    # Core Channels
    # ----------------------------------------------------------------------------------------------
    def get_core_channels(self) -> list[OutputCoreChannel]:
        with self._db.get_db() as db:
            channels = self._channel_repo.fetch_all(db)
        return [OutputCoreChannel.from_db_core_channel(c) for c in channels]

    def get_core_channel(self, channel_id: UUID) -> OutputCoreChannel | None:
        with self._db.get_db() as db:
            channel = self._channel_repo.get_by_id(db, id=channel_id)
        return OutputCoreChannel.from_db_core_channel(channel) if channel else None

    def patch_core_channel(
        self,
        channel_id: UUID,
        name: str | None = None,
        details: dict | None = None,
        zone_id: UUID | None = None,
    ) -> OutputCoreChannel:
        here = "patch_core_channel"
        log_d(here, channel_id)
        with self._db.transaction() as db:
            channel: CoreChannel = self._channel_repo.get_by_id(db, id=channel_id)
            if not channel:
                raise NotFoundError(f"CoreChannel '{channel_id}' not found")
            if name is not None:
                channel.name = name
            if details is not None:
                channel.details = details
            if zone_id is not None:
                zone = self._zone_repo.get_by_id(db, id=zone_id)
                if not zone:
                    raise NotFoundError(f"CoreZone '{zone_id}' not found")
                channel.zone_id = zone_id
            db.flush()
            db.refresh(channel)
        return OutputCoreChannel.from_db_core_channel(channel)

    # ----------------------------------------------------------------------------------------------
    # Groups
    # ----------------------------------------------------------------------------------------------
    def create_group(self, name: str, details: dict | None = None) -> OutputGroup:
        here = "create_group"
        log_d(here, name)
        group = RbacGroup(name=name, details=details or {})
        with self._db.transaction() as db:
            existing = self._group_repo.get_by_name(db, name=name)
            if existing:
                raise BadRequestError(f"Group '{name}' already exists")
            db.add(group)
            db.flush()
        return OutputGroup.from_db_group(group)

    def get_groups(self) -> list[OutputGroup]:
        with self._db.get_db() as db:
            groups = self._group_repo.fetch_all(db)
        return [OutputGroup.from_db_group(g) for g in groups]

    def get_group(self, group_id: UUID) -> OutputGroup | None:
        with self._db.get_db() as db:
            group = self._group_repo.get_by_id(db, id=group_id)
        return OutputGroup.from_db_group(group) if group else None

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
        return OutputGroup.from_db_group(group)

    @log_service_error
    def delete_group(self, group_id: UUID) -> OutputGroup | None:
        here = "delete_group"
        log_w(here, group_id)
        with self._db.transaction() as db:
            group = self._group_repo.get_by_id(db, id=group_id)
            if not group:
                raise NotFoundError(f"Group '{group_id}' not found")

            role_count = db.execute(
                select(func.count(RbacGroupRoles.group_id)).where(RbacGroupRoles.group_id == group_id)
            ).scalar()
            if role_count:
                raise ConflictError(
                    f"Group '{group.name}' cannot be deleted: assigned to {role_count} role(s). "
                    "Remove these role assignments first."
                )

            # Clear group memberships explicitly — SQLAlchemy's unitofwork can't
            # handle PK-as-FK with ondelete=CASCADE (tries to NULL the PK first).
            db.execute(delete(RbacUserGroups.__table__).where(RbacUserGroups.group_id == group_id))

            db.delete(group)
            db.flush()
        return OutputGroup.from_db_group(group)

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
