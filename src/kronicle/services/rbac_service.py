# kronicle/services/rbac_service.py
from uuid import UUID

from kronicle.db.rbac.associations.user_groups import RbacUserGroups
from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.db.rbac.rbac_db_session import RbacDbSession
from kronicle.db.rbac.rbac_engine import RbacEngine
from kronicle.errors.error_types import NotFoundError, UnauthorizedError
from kronicle.schemas.rbac.user_schemas import InputUserLogin, OutputUser, ProcessedUser

"""
FastAPI validates inputs.
RbacService determines transaction scope.
RbacDbSession provides session/connection context.
RbacEngine orchestrates multi-table actions using the session.
Table classes perform simple CRUD and return results.
"""


class RbacService:

    def __init__(
        self,
        rbac_db_session: RbacDbSession,
        rbac_engine=RbacEngine,
    ):
        self._db = rbac_db_session
        self._engine = rbac_engine

    # ----------------------------------------------------------------------------------------------
    # Read-only: fetch user info
    # ----------------------------------------------------------------------------------------------
    def _fetch_user_by_login(self, login_input: InputUserLogin) -> RbacUser:
        with self._db.get_db() as db:  # read-only
            if login_input.is_email:
                email = f"{login_input.login}".lower()
                db_user = self._engine.fetch_user_by_email(db, email)
            else:
                name = login_input.login
                db_user = self._engine.fetch_user_by_name(db, name)
        if not db_user:
            raise NotFoundError("User not found")
        return db_user

    def fetch_user_for_auth(self, login_input: InputUserLogin) -> RbacUser:
        return self._fetch_user_by_login(login_input)

    def fetch_user_info(self, login_input: InputUserLogin) -> OutputUser:
        """
        Fetch the OutputUser for the authenticated user.
        Should only be called after login.
        """
        db_user = self._fetch_user_by_login(login_input)
        return OutputUser.from_db_user(db_user)

    def list_users(self) -> list[OutputUser]:
        with self._db.get_db() as db:  # read-only
            with self._db.get_db() as db:
                users = self._engine.list_users(db)
            return [OutputUser.from_db_user(u) for u in users]

    # ----------------------------------------------------------------------------------------------
    # Write: create user
    # ----------------------------------------------------------------------------------------------
    def create_user(self, user: ProcessedUser) -> OutputUser:
        rbac_user = user.to_db_user()
        with self._db.transaction() as db:
            existing = self._engine.fetch_user_by_email(db=db, email=rbac_user.email)
            if existing:
                raise UnauthorizedError("User already exists")
            db_user = self._engine.create_user(db=db, user=rbac_user)
        out_user = OutputUser.from_db_user(db_user)
        return out_user

    def update_password_hash(self, user_id: UUID, new_hash: str) -> None:
        with self._db.transaction() as db:
            user = db.query(RbacUser).filter(RbacUser.id == user_id).first()
            if user:
                user.password_hash = new_hash

    # ----------------------------------------------------------------------------------------------
    # Subjects / Groups
    # ------------------------------------------------------------------------------------------------
    def get_user_groups(self, user_id: UUID) -> list[UUID]:
        """
        Returns a list of group IDs the user belongs to.
        """
        with self._db.get_db() as session:
            rows = session.query(RbacUserGroups.group_id).filter(RbacUserGroups.user_id == user_id).all()
            return [r.group_id for r in rows]
