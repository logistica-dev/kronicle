# kronicle/schemas/rbac/safe_policy_schemas.py
from __future__ import annotations

from uuid import UUID

from pydantic import Field

from kronicle.db.rbac.links.rbac_access_profile import ChannelAccessProfile, RowAccessProfile, ZoneAccessProfile
from kronicle.db.rbac.links.rbac_policy import ChannelPolicy, RowPolicy, ZonePolicy
from kronicle.schemas.core.safe_ressource_schema import OutputCoreChannel, OutputCoreRow, OutputZone
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
    def from_db(cls, db_obj: ZonePolicy):
        return cls(
            id=db_obj.id,
            name=db_obj.name,
            subject=OutputSubject.from_db(db_obj.subject),
            access_profile=OutputZoneAccessProfile.from_db(db_obj.access_profile),
            is_delegation=db_obj.is_delegation,
        )


class OutputChannelPolicy(OutputPolicy):
    access_profile: OutputChannelAccessProfile

    @classmethod
    def from_db(cls, db_obj: ChannelPolicy):
        return cls(
            id=db_obj.id,
            name=db_obj.name,
            subject=OutputSubject.from_db(db_obj.subject),
            access_profile=OutputChannelAccessProfile.from_db(db_obj.access_profile),
            is_delegation=db_obj.is_delegation,
        )


class OutputRowPolicy(OutputPolicy):
    access_profile: OutputRowAccessProfile

    @classmethod
    def from_db(cls, db_obj: RowPolicy):
        return cls(
            id=db_obj.id,
            name=db_obj.name,
            subject=OutputSubject.from_db(db_obj.subject),
            access_profile=OutputRowAccessProfile.from_db(db_obj.access_profile),
            is_delegation=db_obj.is_delegation,
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
    def from_db(cls, db_obj: ZoneAccessProfile):
        return cls(
            id=db_obj.id,
            name=db_obj.name,
            description=db_obj.description,
            role=OutputRole.from_db(db_obj.role),
            zone=OutputZone.from_db(db_obj.zone),
        )


class OutputChannelAccessProfile(OutputAccessProfile):
    channel: OutputCoreChannel

    @classmethod
    def from_db(cls, db_obj: ChannelAccessProfile):
        return cls(
            id=db_obj.id,
            name=db_obj.name,
            description=db_obj.description,
            role=OutputRole.from_db(db_obj.role),
            channel=OutputCoreChannel.from_db(db_obj.channel),
        )


class OutputRowAccessProfile(OutputAccessProfile):
    row: OutputCoreRow

    @classmethod
    def from_db(cls, db_obj: RowAccessProfile):
        return cls(
            id=db_obj.id,
            name=db_obj.name,
            description=db_obj.description,
            role=OutputRole.from_db(db_obj.role),
            row=OutputCoreRow.from_db(db_obj.row),
        )
