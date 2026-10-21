# kronicle/services/core_service.py
from __future__ import annotations

from collections.abc import Sequence
from uuid import UUID

from kronicle.db.core.models.core_channel import CoreChannel
from kronicle.db.core.models.core_zone import CoreZone
from kronicle.db.rbac.rbac_db_session import RbacDbSession
from kronicle.errors.error_types import BadRequestError, ConflictError, NotFoundError
from kronicle.repo.core.core_channel_repo import CoreChannelRepository
from kronicle.repo.core.core_zone_repo import CoreZoneRepository
from kronicle.repo.hierarchy.hierarchy_service import HierarchyService
from kronicle.repo.hierarchy.zone_hierarchy_repo import ZoneHierarchyRepository
from kronicle.schemas.core.input_ressource_schema import InputCoreChannel
from kronicle.schemas.core.safe_ressource_schema import OutputCoreChannel, OutputZone
from kronicle.utils.dev_logs import log_d, log_i, log_w

mod = "core_svc"

# Name of the zone used when a channel has no explicit zone (sync + migration backfill).
DEFAULT_ZONE_NAME = "default"


class CoreService:
    def __init__(self, core_db_session: RbacDbSession):
        self._db = core_db_session
        self._channel_repo = CoreChannelRepository()
        self._zone_repo = CoreZoneRepository()

        self.zone_hierarchy_service = HierarchyService(
            repo=ZoneHierarchyRepository(),
            max_parents=1,
        )

    # ----------------------------------------------------------------------------------------------
    # Zones
    # ----------------------------------------------------------------------------------------------

    def get_zones(self) -> list[CoreZone]:
        with self._db.get_db() as db:
            return list(self._zone_repo.fetch_all(db))

    def create_zone(self, name: str, details: dict | None = None) -> OutputZone:
        here = f"{mod}.create_zone"
        log_d(here, name)
        zone = CoreZone(name=name, details=details or {})
        with self._db.transaction() as db:
            existing = self._zone_repo.get_by_name(db, name=name)
            if existing:
                raise BadRequestError(f"Zone '{name}' already exists")
            zone = self._zone_repo.add(db, entity=zone)
            return OutputZone.from_db(zone)

    def get_zone(self, zone_id: UUID) -> OutputZone | None:
        with self._db.get_db() as db:
            zone = self._zone_repo.get_by_id(db, id=zone_id)
            return OutputZone.from_db(zone) if zone else None

    def get_zone_by_name(self, name: str) -> OutputZone | None:
        with self._db.get_db() as db:
            zone = self._zone_repo.get_by_name(db, name=name)
            return OutputZone.from_db(zone) if zone else None

    def delete_zone(self, zone_id: UUID) -> OutputZone | None:
        here = f"{mod}.delete_zone"
        log_w(here, zone_id)
        with self._db.transaction() as db:
            zone = self._zone_repo.get_by_id(db, id=zone_id)
            if not zone:
                raise NotFoundError(f"Zone '{zone_id}' not found")
            channels = self._channel_repo.get_by_zone(db, zone_id=zone_id)
            if channels:
                raise ConflictError(f"Zone '{zone_id}' still has {len(channels)} channel(s); delete or move them first")
            zone = self._zone_repo.delete(db, entity=zone)
            return OutputZone.from_db(zone)

    def patch_zone(self, zone_id: UUID, name: str | None = None, details: dict | None = None) -> OutputZone:
        here = f"{mod}.patch_zone"
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
            return OutputZone.from_db(zone)

    # ----------------------------------------------------------------------------------------------
    # Zone Hierarchy
    # ----------------------------------------------------------------------------------------------

    def attach_zone(self, parent_id: UUID, child_id: UUID) -> None:
        here = f"{mod}.attach_zone"
        log_d(here, parent_id, child_id)
        with self._db.transaction() as db:
            parent = self._zone_repo.get_by_id(db, id=parent_id)
            if not parent:
                raise NotFoundError(f"Zone '{parent_id}' not found")
            child = self._zone_repo.get_by_id(db, id=child_id)
            if not child:
                raise NotFoundError(f"Zone '{child_id}' not found")
            if parent_id == child_id:
                raise BadRequestError("Zone cannot be attached to itself")
            try:
                self.zone_hierarchy_service.add_parent(db, parent, child)
            except ValueError as e:
                raise BadRequestError(str(e)) from e

    def detach_zone(self, parent_id: UUID, child_id: UUID) -> None:
        here = f"{mod}.detach_zone"
        log_d(here, parent_id, child_id)
        with self._db.transaction() as db:
            parent = self._zone_repo.get_by_id(db, id=parent_id)
            if not parent:
                raise NotFoundError(f"Zone '{parent_id}' not found")
            child = self._zone_repo.get_by_id(db, id=child_id)
            if not child:
                raise NotFoundError(f"Zone '{child_id}' not found")
            self.zone_hierarchy_service.remove_parent(db, parent, child)

    def get_zone_parents(self, zone_id: UUID) -> list[OutputZone]:
        with self._db.get_db() as db:
            zone = self._zone_repo.get_by_id(db, id=zone_id)
            if not zone:
                raise NotFoundError(f"Zone '{zone_id}' not found")
            parents = self.zone_hierarchy_service.list_parents(db, zone)
            return [OutputZone.from_db(p) for p in parents]

    def get_zone_children(self, zone_id: UUID) -> list[OutputZone]:
        with self._db.get_db() as db:
            zone = self._zone_repo.get_by_id(db, id=zone_id)
            if not zone:
                raise NotFoundError(f"Zone '{zone_id}' not found")
            children = self.zone_hierarchy_service.list_children(db, zone)
            return [OutputZone.from_db(p) for p in children]

    def get_zone_ancestors(self, zone_id: UUID) -> list[OutputZone]:
        with self._db.get_db() as db:
            zone = self._zone_repo.get_by_id(db, id=zone_id)
            if not zone:
                raise NotFoundError(f"Zone '{zone_id}' not found")
            ancestors = self.zone_hierarchy_service.ancestors(db, zone)
            return [OutputZone.from_db(a) for a in ancestors]

    def get_zone_descendants(self, zone_id: UUID) -> list[OutputZone]:
        with self._db.get_db() as db:
            zone = self._zone_repo.get_by_id(db, id=zone_id)
            if not zone:
                raise NotFoundError(f"Zone '{zone_id}' not found")
            descendants = self.zone_hierarchy_service.descendants(db, zone)
            return [OutputZone.from_db(d) for d in descendants]

    # ----------------------------------------------------------------------------------------------
    # Core Channels
    # ----------------------------------------------------------------------------------------------

    def list_core_channel_ids(self) -> set[UUID]:
        with self._db.get_db() as db:
            channels = self._channel_repo.fetch_all(db)
        return {c.id for c in channels}

    def create_missing_core_channels(
        self,
        channels: Sequence[InputCoreChannel],
        default_zone_id: UUID | None = None,
    ) -> list[UUID]:
        here = f"{mod}.create_missing_core_channels"
        existing = self.list_core_channel_ids()
        missing = [c for c in channels if c.id not in existing]
        if not missing:
            log_d(here, "All channels already synced")
            return []

        if default_zone_id is None:
            default_zone_id = self.ensure_default_zone().id

        created: list[UUID] = []
        with self._db.transaction() as db:
            for c in missing:
                core_channel = CoreChannel(
                    id=c.id,
                    name=c.name or f"Channel {c.id.hex}",
                    zone_id=c.zone.id if c.zone else default_zone_id,
                    details=c.details or {},
                )
                db.add(core_channel)
                created.append(c.id)
                log_d(here, f"Created CoreChannel for {c.id}")
            db.flush()
        log_i(here, f"Created {len(created)} CoreChannels")
        return created

    def delete_orphan_core_channels(self, wanted_ids: set[UUID]) -> list[UUID]:
        """Delete CoreChannels whose id is not in ``wanted_ids`` (no ChannelMetadata anymore)."""
        here = f"{mod}.delete_orphan_core_channels"
        wanted = set(wanted_ids)
        with self._db.transaction() as db:
            orphans = {c.id for c in self._channel_repo.fetch_all(db)} - wanted
            if not orphans:
                log_d(here, "No orphan CoreChannels to delete")
                return []
            self._channel_repo.delete_by_ids(db, ids=orphans)
            db.flush()
        log_i(here, f"Deleted {len(orphans)} orphan CoreChannels")
        return list(orphans)

    def ensure_default_zone(self, name: str = DEFAULT_ZONE_NAME) -> CoreZone:
        with self._db.get_db() as db:
            existing = self._zone_repo.get_by_name(db, name=name)
            if existing:
                return existing
        zone = CoreZone(name=name)
        with self._db.transaction() as db:
            zone = self._zone_repo.add(db, entity=zone)
            log_i(f"{mod}.ensure_default_zone", f"Created default zone '{name}'")
        return zone

    def get_core_channels(self, *, zone_id: UUID | None = None) -> list[OutputCoreChannel]:
        with self._db.get_db() as db:
            if zone_id:
                zone = self._zone_repo.get_by_id(db, id=zone_id)
                if not zone:
                    raise NotFoundError(f"Zone '{zone_id}' not found")
                channels = self._channel_repo.get_by_zone(db, zone_id=zone_id)
            else:
                channels = self._channel_repo.fetch_all(db)
            return [OutputCoreChannel.from_db(c) for c in channels]

    def get_core_channel(self, channel_id: UUID) -> OutputCoreChannel | None:
        with self._db.get_db() as db:
            channel = self._channel_repo.get_by_id(db, id=channel_id)
            return OutputCoreChannel.from_db(channel) if channel else None

    def get_core_channel_by_name(self, name: str) -> OutputCoreChannel | None:
        with self._db.get_db() as db:
            try:
                channel = self._channel_repo.get_by_name(db, name=name)
                return OutputCoreChannel.from_db(channel) if channel else None
            except NotFoundError:
                return None

    def create_core_channel(self, channel: InputCoreChannel, zone_id: UUID) -> OutputCoreChannel:
        with self._db.transaction() as db:
            core_channel = CoreChannel(
                id=channel.id,
                name=channel.name or f"Channel {channel.id.hex}",
                zone_id=zone_id,
                details=channel.details or {},
            )
            core_channel = self._channel_repo.add(db, entity=core_channel)
            return OutputCoreChannel.from_db(core_channel)

    def ensure_channel_in_zone(self, channel: InputCoreChannel, zone_id: UUID) -> bool:
        zone = self.get_zone(zone_id)
        if not zone:
            raise NotFoundError(f"Zone {zone_id} not found")
        existing = self.get_core_channel(channel.id)
        if existing:
            if existing.zone and existing.zone.id != zone_id:
                raise ConflictError(f"Channel {channel.id} belongs to zone {existing.zone.id}, not {zone_id}")
            return False
        name = channel.name or f"Channel {channel.id.hex}"
        existing_by_name = self.get_core_channel_by_name(name)
        if existing_by_name:
            zone_ref = existing_by_name.zone.id if existing_by_name.zone else "no zone"
            raise ConflictError(
                f"Channel name '{name}' already exists for channel {existing_by_name.id} in zone {zone_ref}"
            )
        self.create_core_channel(channel, zone_id=zone.id)
        return True

    def delete_core_channel(self, channel_id: UUID) -> OutputCoreChannel | None:
        with self._db.transaction() as db:
            channel: CoreChannel = self._channel_repo.get_by_id(db, id=channel_id)
            if not channel:
                return None
            channel = self._channel_repo.delete(db, entity=channel)
            return OutputCoreChannel.from_db(channel)

    def patch_core_channel(
        self,
        channel_id: UUID,
        name: str | None = None,
        details: dict | None = None,
        zone_id: UUID | None = None,
    ) -> OutputCoreChannel:
        here = f"{mod}.patch_core_channel"
        log_d(here, channel_id)
        with self._db.transaction() as db:
            channel: CoreChannel = self._channel_repo.get_by_id(db, id=channel_id)
            if not channel:
                raise NotFoundError(f"CoreChannel '{channel_id}' not found")
            if name is not None:
                channel.name = name
            if details is not None:
                channel.details = details
            # Only available to users with setup rights
            if zone_id is not None:
                zone = self._zone_repo.get_by_id(db, id=zone_id)
                if not zone:
                    raise NotFoundError(f"CoreZone '{zone_id}' not found")
                channel.zone_id = zone_id
            db.flush()
            db.refresh(channel)
            return OutputCoreChannel.from_db(channel)
