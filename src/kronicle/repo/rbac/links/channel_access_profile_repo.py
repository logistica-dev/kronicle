# kronicle/repo/rbac/links/channel_access_profile_repo.py
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from kronicle.db.rbac.links.rbac_access_profile import ChannelAccessProfile
from kronicle.repo.kronicle_repo import KronicleRepository, log_repo_error


class ChannelAccessProfileRepository(KronicleRepository[ChannelAccessProfile]):
    model = ChannelAccessProfile

    @log_repo_error
    def get_by_role_and_channel(self, db: Session, *, role_id: UUID, channel_id: UUID) -> ChannelAccessProfile | None:
        stmt = select(self.model).where(self.model.role_id == role_id, self.model.channel_id == channel_id)
        return db.execute(stmt).scalar_one_or_none()
