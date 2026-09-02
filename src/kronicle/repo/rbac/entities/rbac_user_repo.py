# kronicle/repo/rbac/entities/rbac_user_repo.py
from collections.abc import Sequence
from uuid import UUID

from sqlalchemy.orm import Session
from sqlalchemy.sql import select

from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.errors.error_types import NotFoundError
from kronicle.repo.kronicle_repo import KronicleRepository, log_repo_error


class RbacUserRepository(KronicleRepository[RbacUser]):

    model = RbacUser

    # ----------------------------------------------------------------------------------------------
    # Internal helper pattern (important for consistency)
    # ----------------------------------------------------------------------------------------------
    def _apply_superuser_filter(
        self,
        stmt,
        *,
        include_inactive: bool = False,
        include_superusers: bool = False,
    ):
        if not include_inactive:
            stmt = stmt.where(RbacUser.is_active.is_(True))
        if not include_superusers:
            stmt = stmt.where(RbacUser.is_superuser.is_(False))
        return stmt

    # ----------------------------------------------------------------------------------------------
    # Fetch methods
    # ----------------------------------------------------------------------------------------------
    @log_repo_error
    def get_by_id(
        self,
        db: Session,
        *,
        id: UUID,
        include_inactive: bool = False,
        include_superusers: bool = False,
    ) -> RbacUser | None:
        stmt = select(RbacUser).where(RbacUser.id == id)
        stmt = self._apply_superuser_filter(
            stmt,
            include_inactive=include_inactive,
            include_superusers=include_superusers,
        )
        return db.execute(stmt).scalar_one_or_none()

    @log_repo_error
    def get_by_email(
        self,
        db: Session,
        *,
        email: str,
        include_inactive: bool = False,
        include_superusers: bool = False,
    ) -> RbacUser | None:
        stmt = select(RbacUser).where(RbacUser.email == email)
        stmt = self._apply_superuser_filter(
            stmt,
            include_inactive=include_inactive,
            include_superusers=include_superusers,
        )
        return db.execute(stmt).scalar_one_or_none()

    @log_repo_error
    def get_by_name(
        self,
        db: Session,
        *,
        name: str,
        include_inactive: bool = False,
        include_superusers: bool = False,
    ) -> RbacUser | None:
        stmt = select(RbacUser).where(RbacUser.name == name)
        stmt = self._apply_superuser_filter(
            stmt,
            include_inactive=include_inactive,
            include_superusers=include_superusers,
        )
        return db.execute(stmt).scalar_one_or_none()

    @log_repo_error
    def get_by_external_id(
        self,
        db: Session,
        *,
        external_id: str,
        include_inactive: bool = False,
        include_superusers: bool = False,
    ) -> RbacUser | None:
        stmt = select(RbacUser).where(RbacUser.external_id == external_id)
        stmt = self._apply_superuser_filter(
            stmt,
            include_inactive=include_inactive,
            include_superusers=include_superusers,
        )
        return db.execute(stmt).scalar_one_or_none()

    @log_repo_error
    def fetch_all(
        self,
        db: Session,
        *,
        include_inactive: bool = False,
        include_superusers: bool = False,
    ) -> Sequence[RbacUser]:
        stmt = select(RbacUser)
        stmt = self._apply_superuser_filter(
            stmt,
            include_superusers=include_superusers,
            include_inactive=include_inactive,
        )
        return db.execute(stmt).scalars().all()

    # ----------------------------------------------------------------------------------------------
    # Write methods
    # ----------------------------------------------------------------------------------------------
    @log_repo_error
    def create_user(self, db: Session, *, user: RbacUser) -> RbacUser:
        return self.add(db, entity=user)

    @log_repo_error
    def update_user(self, db: Session, *, user: RbacUser) -> RbacUser:
        return self.save(db, entity=user)

    @log_repo_error
    def update_password_hash(
        self,
        db: Session,
        *,
        user_id: UUID,
        new_hash: str,
    ) -> None:
        user = self.get_by_id(db, id=user_id)
        if not user:
            raise NotFoundError("User not found")
        user.password_hash = new_hash

    def delete_user(self, db: Session, *, user: RbacUser) -> RbacUser:
        return self.delete_by_id_returning(db, id=user.id)
