# kronicle/repo/rbac/entities/row_policy_repo.py
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from kronicle.db.rbac.links.rbac_access_profile import RowAccessProfile
from kronicle.db.rbac.links.rbac_policy import RowPolicy
from kronicle.db.rbac.models.rbac_role import RbacRole
from kronicle.repo.kronicle_repo import KronicleRepository


class RowPolicyRepository(KronicleRepository[RowPolicy]):
    model = RowPolicy

    def get_policies_for_row(self, db: Session, *, row_id: UUID) -> list[RowPolicy]:
        stmt = (
            select(self.model)
            .join(RowAccessProfile, self.model.access_profile_id == RowAccessProfile.id)
            .join(RbacRole, RowAccessProfile.role_id == RbacRole.id)
            .where(RowAccessProfile.row_id == row_id)
        )
        return list(db.execute(stmt).scalars().all())
