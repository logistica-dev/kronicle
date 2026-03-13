# kronicle/auth/auth_service.py
from kronicle.auth.jwt_service import JWTService
from kronicle.auth.pwd.pwd_manager import PasswordManager
from kronicle.db.rbac.models.rbac_user import RbacUser
from kronicle.errors.error_types import NotFoundError, UnauthorizedError
from kronicle.schemas.rbac.input_user_schemas import InputUserChangePwd, InputUserLogin
from kronicle.schemas.rbac.safe_user_schemas import OutputUser
from kronicle.services.rbac_service import RbacService
from kronicle.utils.dev_logs import log_w


class AuthService:
    """Authentication & user management service (synchronous)."""

    def __init__(self, jwt_service: JWTService, rbac_service: RbacService):
        if jwt_service is None:
            raise RuntimeError("[AuthService] JwtService not initialized. Call init() from main app first.")
        if rbac_service is None:
            raise RuntimeError("[AuthService] RbacService not initialized. Call init() from main app first.")
        self._jwt_service = jwt_service
        self._pwd_manager = PasswordManager.get_instance()

        self._rbac_service = rbac_service

    # -----------------------------
    # Login / JWT generation
    # -----------------------------
    def login(self, login_input: InputUserLogin) -> str:
        """
        Authenticate a user by username or email and return a JWT.
        Responsabilities:
        - Password verification
        - Rehash logic
        - JWT creation
        - Authentication flow

        Raises UnauthorizedError if credentials are invalid.
        """
        here = "login"
        try:
            db_user: RbacUser | None = self._rbac_service.fetch_user_for_auth(login_input)
        except NotFoundError as e:
            log_w(here, "User not found")
            raise UnauthorizedError("Invalid credentials") from e

        if not db_user or not db_user.is_active or not db_user.password_hash:
            log_w(here, "User does not exist")
            raise UnauthorizedError("Invalid credentials")

        # Verify raw password against stored hash
        try:
            if not self._pwd_manager.verify_password(db_user.password_hash, login_input.password):
                raise UnauthorizedError("Invalid credentials")
        except Exception as e:
            log_w(here, "Password verification failed (mismatch)")
            raise UnauthorizedError("Invalid credentials") from e
        # Rehash if Argon2 parameters changed
        if self._pwd_manager.needs_rehash(db_user.password_hash):
            new_hash = self._pwd_manager.rehash_password(db_user.password_hash, login_input.password)
            self._rbac_service.update_password_hash(db_user.id, new_hash)

        out_user = OutputUser.from_db_user(db_user=db_user)
        # Return JWT
        return self._jwt_service.create_access_token(out_user)

    def change_password(self, creds: InputUserChangePwd):
        here = "change_password"
        try:
            db_user: RbacUser | None = self._rbac_service.fetch_user_for_auth(creds)
        except NotFoundError as e:
            log_w(here, "User not found")
            raise UnauthorizedError("Invalid credentials") from e

        if not db_user or not db_user.is_active or not db_user.password_hash:
            log_w(here, "User does not exist")
            raise UnauthorizedError("Invalid credentials")

        # Verify raw password against stored hash
        try:
            if not self._pwd_manager.verify_password(db_user.password_hash, creds.password):
                raise UnauthorizedError("Invalid credentials")
        except Exception as e:
            log_w(here, "Password verification failed (mismatch)")
            raise UnauthorizedError("Invalid credentials") from e

        new_hash = self._pwd_manager.hash_password(creds.new_password)

        self._rbac_service.update_password_hash(db_user.id, new_hash)

        # Return JWT
        out_user = OutputUser.from_db_user(db_user=db_user)
        return self._jwt_service.create_access_token(out_user)
