# kronicle/schemas/rbac/safe_policy_schemas.py
from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel

from kronicle.db.rbac.links.rbac_access_profile import ChannelAccessProfile, ZoneAccessProfile
from kronicle.db.rbac.models.rbac_policy import ChannelPolicy, ZonePolicy


class OutputPolicy(BaseModel):
    id: UUID
    subject_id: UUID
    role_id: UUID
    role_name: str | None = None
    is_delegation: bool = False


class OutputZonePolicy(OutputPolicy):
    zone_id: UUID
    zone_name: str | None = None

    @classmethod
    def from_db(cls, policy: ZonePolicy):
        profile = policy.access_profile
        return cls(
            id=policy.id,
            subject_id=policy.subject_id,
            role_id=profile.role_id,
            role_name=profile.role.name if profile.role else None,
            zone_id=profile.zone_id,
            zone_name=profile.zone.name if profile.zone else None,
            is_delegation=policy.is_delegation,
        )


class OutputChannelPolicy(OutputPolicy):
    channel_id: UUID
    channel_name: str | None = None

    @classmethod
    def from_db(cls, policy: ChannelPolicy):
        profile = policy.access_profile
        return cls(
            id=policy.id,
            subject_id=policy.subject_id,
            role_id=profile.role_id,
            role_name=profile.role.name if profile.role else None,
            channel_id=profile.channel_id,
            channel_name=profile.channel.name if profile.channel else None,
            is_delegation=policy.is_delegation,
        )


class OutputAccessProfile(BaseModel):
    id: UUID
    role_id: UUID
    role_name: str | None = None
    description: str | None = None


class OutputZoneAccessProfile(OutputAccessProfile):
    zone_id: UUID
    zone_name: str | None = None

    @classmethod
    def from_db(cls, profile: ZoneAccessProfile):
        return cls(
            id=profile.id,
            role_id=profile.role_id,
            role_name=profile.role.name if profile.role else None,
            zone_id=profile.zone_id,
            zone_name=profile.zone.name if profile.zone else None,
            description=profile.description,
        )


class OutputChannelAccessProfile(OutputAccessProfile):
    channel_id: UUID
    channel_name: str | None = None

    @classmethod
    def from_db(cls, profile: ChannelAccessProfile):
        return cls(
            id=profile.id,
            role_id=profile.role_id,
            role_name=profile.role.name if profile.role else None,
            channel_id=profile.channel_id,
            channel_name=profile.channel.name if profile.channel else None,
            description=profile.description,
        )
