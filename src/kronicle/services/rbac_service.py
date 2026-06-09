# kronicle/services/rbac_service.py
from collections.abc import Sequence
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.session import Session

from kronicle.db.core.links.zone_hierarchy import ZoneHierarchy
from kronicle.db.core.models.core_channel import CoreChannel
from kronicle.db.core.models.core_zone import CoreZone
from kronicle.db.rbac.links.group_hierarchy import RbacGroupHierarchy
from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.db.rbac.rbac_db_session import RbacDbSession
from kronicle.errors.error_types import BadRequestError, NotFoundError, UnauthorizedError
from kronicle.repo.core.core_channel_repo import CoreChannelRepository
from kronicle.repo.core.core_zone_repo import CoreZoneRepository
from kronicle.repo.hierarchy.hierarchy_engine import HierarchyEngine
from kronicle.repo.hierarchy.hierarchy_service import HierarchyService
from kronicle.repo.hierarchy.zone_hierarchy_repo import ZoneHierarchyRepository
from kronicle.repo.rbac.entities.rbac_group_repo import RbacGroupRepository
from kronicle.repo.rbac.entities.rbac_user_repo import RbacUserRepository
from kronicle.repo.rbac.links.rbac_user_group_repo import RbacUserGroupRepository
from kronicle.schemas.core.safe_core_channel_schemas import OutputCoreChannel
from kronicle.schemas.core.safe_zone_schemas import OutputZone
from kronicle.schemas.rbac.input_user_schemas import InputUserLogin
from kronicle.schemas.rbac.safe_group_schemas import OutputGroup
from kronicle.schemas.rbac.safe_user_schemas import OutputUser, ProcessedUser
from kronicle.utils.dev_logs import log_d, log_i, log_w

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

    def list_users(self) -> list[OutputUser]:
        with self._db.get_db() as db:  # read-only
            users = self._user_repo.fetch_all(db)
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

    def delete_group(self, group_id: UUID) -> OutputGroup | None:
        here = "delete_group"
        log_w(here, group_id)
        with self._db.transaction() as db:
            group = self._group_repo.get_by_id(db, id=group_id)
            if not group:
                raise NotFoundError(f"Group '{group_id}' not found")
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
