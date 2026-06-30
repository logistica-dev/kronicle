# kronicle/repo/rbac/links/zone_access_profile_repo.py
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from kronicle.db.rbac.links.rbac_access_profile import ZoneAccessProfile
from kronicle.repo.kronicle_repo import KronicleRepository, log_repo_error


class ZoneAccessProfileRepository(KronicleRepository[ZoneAccessProfile]):
    model = ZoneAccessProfile

    @log_repo_error
    def get_by_role_and_zone(self, db: Session, *, role_id: UUID, zone_id: UUID) -> ZoneAccessProfile | None:
        stmt = select(self.model).where(self.model.role_id == role_id, self.model.zone_id == zone_id)
        return db.execute(stmt).scalar_one_or_none()
