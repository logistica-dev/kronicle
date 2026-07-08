# kronicle/repo/rbac/entities/zone_policy_repo.py
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from kronicle.db.rbac.links.rbac_access_profile import ZoneAccessProfile
from kronicle.db.rbac.links.rbac_policy import ZonePolicy
from kronicle.db.rbac.models.rbac_role import RbacRole
from kronicle.repo.kronicle_repo import KronicleRepository


class ZonePolicyRepository(KronicleRepository[ZonePolicy]):
    model = ZonePolicy

    def get_by_subject_and_access_profile(
        self, db: Session, *, subject_id: UUID, access_profile_id: UUID
    ) -> ZonePolicy | None:
        stmt = select(self.model).where(
            self.model.subject_id == subject_id,
            self.model.access_profile_id == access_profile_id,
        )
        return db.execute(stmt).scalar_one_or_none()

    def get_policies_for_zone(self, db: Session, *, zone_id: UUID) -> list[ZonePolicy]:
        stmt = (
            select(self.model)
            .join(ZoneAccessProfile, self.model.access_profile_id == ZoneAccessProfile.id)
            .join(RbacRole, ZoneAccessProfile.role_id == RbacRole.id)
            .where(ZoneAccessProfile.zone_id == zone_id)
        )
        return list(db.execute(stmt).scalars().all())
