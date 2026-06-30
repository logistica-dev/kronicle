# kronicle/repo/rbac/entities/channel_policy_repo.py
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from kronicle.db.rbac.links.rbac_access_profile import ChannelAccessProfile
from kronicle.db.rbac.models.rbac_policy import ChannelPolicy
from kronicle.db.rbac.models.rbac_role import RbacRole
from kronicle.repo.kronicle_repo import KronicleRepository


class ChannelPolicyRepository(KronicleRepository[ChannelPolicy]):
    model = ChannelPolicy

    def get_policies_for_channel(self, db: Session, *, channel_id: UUID) -> list[ChannelPolicy]:
        stmt = (
            select(self.model)
            .join(ChannelAccessProfile, self.model.access_profile_id == ChannelAccessProfile.id)
            .join(RbacRole, ChannelAccessProfile.role_id == RbacRole.id)
            .where(ChannelAccessProfile.channel_id == channel_id)
        )
        return list(db.execute(stmt).scalars().all())
