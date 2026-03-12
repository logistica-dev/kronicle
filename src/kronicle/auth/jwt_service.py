# kronicle/auth/jwt_service.py
from datetime import datetime, timedelta, timezone

from jose import jwt

from kronicle.deps.settings import JWTSettings
from kronicle.errors.error_types import UnauthorizedError
from kronicle.schemas.rbac.user_schemas import OutputUser

mod = "jwt"


class JWTService:
    def __init__(self, jwt_conf: JWTSettings):
        self._secret = jwt_conf.get_secret()
        self._algo = jwt_conf.algorithm
        self._exp_minutes = jwt_conf.expiration_minutes

    def _get_payload_from_out_user(self, user: OutputUser) -> dict:
        # here = f"{mod}_get_payload_from_out_user"
        expire = datetime.now(timezone.utc) + timedelta(minutes=self._exp_minutes)
        payload = {
            "sub": str(user.id),
            "exp": int(expire.timestamp()),
            "roles": ["writer"],  # TODO
        }
        if user.is_su:
            payload["is_superuser"] = True
        return payload

    def create_access_token(self, user: OutputUser) -> str:
        payload = self._get_payload_from_out_user(user)
        return jwt.encode(payload, self._secret, algorithm=self._algo)

    def decode_token(self, token: str) -> dict:
        try:
            return jwt.decode(token, self._secret, algorithms=[self._algo])
        except Exception as e:
            raise UnauthorizedError(f"Invalid token: {e}") from e

    def verify_token(self, token: str) -> bool:
        try:
            self.decode_token(token)
            return True
        except UnauthorizedError:
            return False
