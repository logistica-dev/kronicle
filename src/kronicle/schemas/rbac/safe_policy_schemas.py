# kronicle/schemas/rbac/safe_policy_schemas.py
from __future__ import annotations

from uuid import UUID

from pydantic import Field

from kronicle.db.rbac.links.rbac_access_profile import ChannelAccessProfile, RowAccessProfile, ZoneAccessProfile
from kronicle.db.rbac.links.rbac_policy import ChannelPolicy, RowPolicy, ZonePolicy
from kronicle.schemas.core.safe_ressource_schema import OutputCoreChannel, OutputZone
from kronicle.schemas.output_schema import OutputSchema
from kronicle.schemas.rbac.input_subject_schemas import SubjectType
from kronicle.schemas.rbac.safe_role_schemas import OutputRole


# --------------------------------------------------------------------------------------------------
# OutputSubject
# --------------------------------------------------------------------------------------------------
class OutputSubject(OutputSchema):
    type: SubjectType
    user_id: UUID | None = Field(default=None, description="UUID of the user")
    group_id: UUID | None = Field(default=None, description="UUID of the group")


# --------------------------------------------------------------------------------------------------
# OutputPolicy
# --------------------------------------------------------------------------------------------------
class OutputPolicy(OutputSchema):
    subject: OutputSubject
    is_delegation: bool = False


class OutputZonePolicy(OutputPolicy):
    access_profile: OutputZoneAccessProfile

    @classmethod
    def from_db(cls, row: ZonePolicy):
        return cls(
            id=row.id,
            name=row.name,
            subject=OutputSubject.from_db(row.subject),
            access_profile=OutputZoneAccessProfile.from_db(row.access_profile),
            is_delegation=row.is_delegation,
        )


class OutputChannelPolicy(OutputPolicy):
    access_profile: OutputChannelAccessProfile

    @classmethod
    def from_db(cls, row: ChannelPolicy):
        return cls(
            id=row.id,
            name=row.name,
            subject=OutputSubject.from_db(row.subject),
            access_profile=OutputChannelAccessProfile.from_db(row.access_profile),
            is_delegation=row.is_delegation,
        )


class OutputRowPolicy(OutputPolicy):
    access_profile: OutputRowAccessProfile

    @classmethod
    def from_db(cls, row: RowPolicy):
        return cls(
            id=row.id,
            name=row.name,
            subject=OutputSubject.from_db(row.subject),
            access_profile=OutputRowAccessProfile.from_db(row.access_profile),
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
