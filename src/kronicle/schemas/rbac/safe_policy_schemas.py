# kronicle/schemas/rbac/safe_policy_schemas.py
from __future__ import annotations

from uuid import UUID

from kronicle.db.rbac.links.rbac_access_profile import ChannelAccessProfile, RowAccessProfile, ZoneAccessProfile
from kronicle.db.rbac.links.rbac_policy import ChannelPolicy, RowPolicy, ZonePolicy
from kronicle.schemas.core.safe_ressource_schema import OutputCoreChannel, OutputZone
from kronicle.schemas.output_schema import OutputSchema
from kronicle.schemas.rbac.safe_role_schemas import OutputRole


# --------------------------------------------------------------------------------------------------
# OutputSubject
# --------------------------------------------------------------------------------------------------
class OutputSubject(OutputSchema):
    subject_type: str


# --------------------------------------------------------------------------------------------------
# OutputPolicy
# --------------------------------------------------------------------------------------------------
class OutputPolicy(OutputSchema):
    subject: OutputSubject
    role: OutputRole
    is_delegation: bool = False


class OutputZonePolicy(OutputPolicy):
    profile: OutputZoneAccessProfile
    zone: OutputZone

    @classmethod
    def from_db(cls, row: ZonePolicy):
        profile = row.access_profile
        return cls(
            id=row.id,
            name=row.name,
            subject=OutputSubject.from_db(row.subject),
            role=OutputRole.from_db(profile.role),
            profile=OutputZoneAccessProfile.from_db(profile),
            zone=OutputZone.from_db(profile.zone),
            is_delegation=row.is_delegation,
        )


class OutputChannelPolicy(OutputPolicy):
    profile: OutputChannelAccessProfile
    channel: OutputCoreChannel

    @classmethod
    def from_db(cls, row: ChannelPolicy):
        profile = row.access_profile
        return cls(
            id=row.id,
            name=row.name,
            subject=OutputSubject.from_db(row.subject),
            role=OutputRole.from_db(profile.role),
            profile=OutputChannelAccessProfile.from_db(profile),
            channel=OutputCoreChannel.from_db(profile.channel),
            is_delegation=row.is_delegation,
        )


class OutputRowPolicy(OutputPolicy):
    profile: OutputRowAccessProfile
    row_id: UUID

    @classmethod
    def from_db(cls, row: RowPolicy):
        profile = row.access_profile
        return cls(
            id=row.id,
            name=row.name,
            subject=OutputSubject.from_db(row.subject),
            role=OutputRole.from_db(profile.role),
            profile=OutputRowAccessProfile.from_db(profile),
            row_id=profile.row_id,
            is_delegation=row.is_delegation,
        )


# --------------------------------------------------------------------------------------------------
# OutputAccessProfile
# --------------------------------------------------------------------------------------------------
class OutputAccessProfile(OutputSchema):
    role: OutputRole
    description: str | None = None


class OutputZoneAccessProfile(OutputAccessProfile):
    zone: OutputZone

    @classmethod
    def from_db(cls, row: ZoneAccessProfile):
        return cls(
            id=row.id,
            name=row.name,
            description=row.description,
            role=OutputRole.from_db(row.role),
            zone=OutputZone.from_db(row.zone),
        )


class OutputChannelAccessProfile(OutputAccessProfile):
    channel: OutputCoreChannel

    @classmethod
    def from_db(cls, row: ChannelAccessProfile):
        return cls(
            id=row.id,
            name=row.name,
            description=row.description,
            role=OutputRole.from_db(row.role),
            channel=OutputCoreChannel.from_db(row.channel),
        )


class OutputRowAccessProfile(OutputAccessProfile):
    row_id: UUID

    @classmethod
    def from_db(cls, row: RowAccessProfile):
        return cls(
            id=row.id,
            name=row.name,
            description=row.description,
            role=OutputRole.from_db(row.role),
            row_id=row.row_id,
        )
