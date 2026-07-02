# kronicle/repo/rbac/links/row_access_profile_repo.py
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from kronicle.db.rbac.links.rbac_access_profile import RowAccessProfile
from kronicle.repo.kronicle_repo import KronicleRepository, log_repo_error


class RowAccessProfileRepository(KronicleRepository[RowAccessProfile]):
    model = RowAccessProfile

    @log_repo_error
    def get_by_role_and_row(self, db: Session, *, role_id: UUID, row_id: UUID) -> RowAccessProfile | None:
        stmt = select(self.model).where(self.model.role_id == role_id, self.model.row_id == row_id)
        return db.execute(stmt).scalar_one_or_none()

    @log_repo_error
    def create(self, db: Session, *, role_id: UUID, row_id: UUID) -> RowAccessProfile:
        profile = RowAccessProfile(role_id=role_id, row_id=row_id)
        db.add(profile)
        db.flush()
        return profile
