# kronicle/repo/core/core_channel_repo.py

from collections.abc import Sequence
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from kronicle.db.core.models.core_channel import CoreChannel
from kronicle.repo.kronicle_repo import KronicleRepository, log_repo_error


class CoreChannelRepository(KronicleRepository[CoreChannel]):

    model = CoreChannel

    @log_repo_error
    def get_by_zone(self, db: Session, *, zone_id: UUID) -> Sequence[CoreChannel]:
        stmt = select(self.model).where(self.model.zone_id == zone_id)
        return db.execute(stmt).scalars().all()
