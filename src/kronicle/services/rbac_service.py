# kronicle/services/rbac_service.py
from uuid import UUID

from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.db.rbac.rbac_db_session import RbacDbSession
from kronicle.db.rbac.rbac_engine import RbacEngine
from kronicle.errors.error_types import BadRequestError, NotFoundError, UnauthorizedError
from kronicle.schemas.rbac.input_user_schemas import InputUserLogin
from kronicle.schemas.rbac.safe_user_schemas import OutputUser, ProcessedUser
from kronicle.utils.dev_logs import log_d

"""
FastAPI validates inputs.
RbacService determines transaction scope.
RbacDbSession provides session/connection context.
RbacEngine orchestrates multi-table actions using the session.
Table classes perform simple CRUD and return results.
"""

mod = "rbacs"


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

    def fetch_user_by_email(self, email: str) -> OutputUser | None:
        with self._db.get_db() as db:  # read-only
            db_user = self._engine.fetch_user_by_email(db, email=email)
        return OutputUser.from_db_user(db_user) if db_user else None

    def fetch_user_by_name(self, name: str) -> OutputUser | None:
        with self._db.get_db() as db:  # read-only
            db_user = self._engine.fetch_user_by_name(db, name=name)
        return OutputUser.from_db_user(db_user) if db_user else None

    def fetch_user_by_external_id(self, orcid: str) -> OutputUser | None:
        with self._db.get_db() as db:  # read-only
            db_user = self._engine.fetch_user_by_external_id(db, external_id=orcid)
        return OutputUser.from_db_user(db_user) if db_user else None

    def list_users(self) -> list[OutputUser]:
        with self._db.get_db() as db:  # read-only
            users = self._engine.list_users(db)
        return [OutputUser.from_db_user(u) for u in users]

    # ----------------------------------------------------------------------------------------------
    # Write: create user
    # ----------------------------------------------------------------------------------------------
    def create_user(self, user: ProcessedUser) -> OutputUser:
        here = "create_usr"
        log_d(here, user.email)
        if not user.password_hash:
            raise BadRequestError("Input user password should be provided")
        rbac_user = user.to_db_user()

        with self._db.transaction() as db:
            existing = self._engine.fetch_user_by_email(db=db, email=rbac_user.email)
            if existing:
                raise UnauthorizedError("User already exists")
            log_d(here, rbac_user.name)
            db_user = self._engine.create_user(db=db, user=rbac_user)
        out_user = OutputUser.from_db_user(db_user)
        return out_user

    def patch_user(self, user: ProcessedUser) -> OutputUser:
        here = "patch_user"
        log_d(here, user.email)
        with self._db.transaction() as db:
            db_user: RbacUser = self._engine.fetch_user_by_email(db=db, email=user.email)
            if not db_user:
                raise UnauthorizedError("User doesn't exists")
            updated = False
            if user.name is not None:
                db_user.name = user.name
                updated = True
            if user.full_name is not None:
                db_user.full_name = user.full_name
                updated = True
            if user.orcid is not None:
                db_user.external_id = user.orcid
                updated = True
            if updated:
                db.commit()
            db.refresh(db_user)
        return OutputUser.from_db_user(db_user)

    def delete_user(self, user: ProcessedUser) -> OutputUser:
        here = "delete_user"
        log_d(here, user.email)
        with self._db.transaction() as db:
            db_user: RbacUser = self._engine.fetch_user_by_email(db=db, email=user.email)
            if not db_user:
                raise UnauthorizedError("User doesn't exists")
            db_user.is_active = False
            db.commit()
            db.refresh(db_user)
        return OutputUser.from_db_user(db_user)

    def update_password_hash(self, user_id: UUID, new_hash: str) -> None:
        with self._db.transaction() as db:
            self._engine.update_password_hash(db, user_id, new_hash)

    # ----------------------------------------------------------------------------------------------
    # Subjects / Groups
    # ----------------------------------------------------------------------------------------------
    def get_user_groups(self, user_id: UUID) -> list[UUID]:
        """
        Returns a list of group IDs the user belongs to.
        """
        with self._db.get_db() as db:
            return self._engine.get_user_groups(db, user_id=user_id)
