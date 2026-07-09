# kronicle/repo/rbac/entities/channel_policy_repo.py
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from kronicle.db.rbac.links.rbac_access_profile import ChannelAccessProfile
from kronicle.db.rbac.links.rbac_policy import ChannelPolicy
from kronicle.db.rbac.models.rbac_role import RbacRole
from kronicle.repo.kronicle_repo import KronicleRepository


class ChannelPolicyRepository(KronicleRepository[ChannelPolicy]):
    model = ChannelPolicy

    def get_by_subject_and_access_profile(
        self, db: Session, *, subject_id: UUID, access_profile_id: UUID
    ) -> ChannelPolicy | None:
        stmt = select(self.model).where(
            self.model.subject_id == subject_id,
            self.model.access_profile_id == access_profile_id,
        )
        return db.execute(stmt).scalar_one_or_none()

    def get_policies_for_channel(self, db: Session, *, channel_id: UUID) -> list[ChannelPolicy]:
        stmt = (
            select(self.model)
            .join(ChannelAccessProfile, self.model.access_profile_id == ChannelAccessProfile.id)
            .join(RbacRole, ChannelAccessProfile.role_id == RbacRole.id)
            .where(ChannelAccessProfile.channel_id == channel_id)
        )
        return list(db.execute(stmt).scalars().all())
