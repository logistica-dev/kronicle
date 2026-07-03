# kronicle/repo/rbac/entities/rbac_subject_repo.py

from sqlalchemy.orm import Session

from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_subject import RbacSubject
from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.repo.kronicle_repo import KronicleRepository, log_repo_error


class RbacSubjectRepository(KronicleRepository[RbacSubject]):

    model = RbacSubject

    @log_repo_error
    def ensure_from_user(self, db: Session, *, user: RbacUser) -> RbacSubject:
        existing: RbacSubject | None = self.get_by_id(db, id=user.id)
        if existing:
            return existing
        subject = RbacSubject(id=user.id, type="user", user_id=user.id, name=user.name)
        self.add(db, entity=subject)
        return subject

    @log_repo_error
    def ensure_from_group(self, db: Session, *, group: RbacGroup) -> RbacSubject:
        existing: RbacSubject | None = self.get_by_id(db, id=group.id)
        if existing:
            return existing
        subject = RbacSubject(id=group.id, type="group", group_id=group.id, name=group.name)
        self.add(db, entity=subject)
        return subject
