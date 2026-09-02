# kronicle/repo/core/core_row_repo.py

from uuid import UUID

from sqlalchemy import select

from kronicle.db.core.models.core_row import CoreRow
from kronicle.repo.kronicle_repo import KronicleRepository, log_repo_error


class CoreRowRepository(KronicleRepository[CoreRow]):

    model = CoreRow

    @log_repo_error
    def get_by_channel_and_row_id(
        self,
        db,
        *,
        channel_id: UUID,
        timeseries_row_id: int,
    ) -> CoreRow | None:
        return db.execute(
            select(CoreRow).where(
                CoreRow.channel_id == channel_id,
                CoreRow.timeseries_row_id == timeseries_row_id,
            )
        ).scalar_one_or_none()
