"""
Security utilities for JWT token handling
"""

from datetime import timedelta
from typing import Any, Final

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from jose import ExpiredSignatureError, JWTError, jwt
from passlib.context import CryptContext

from kronicle.core.ini_settings import conf
from kronicle.types.errors import UnauthorizedError
from kronicle.types.iso_datetime import IsoDateTime

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
jwt_settings = conf.jwt

JWT_EXPIRATION_MINUTES = Final[5]

JWT_PRVK = Ed25519PrivateKey.generate()
JWT_PUBK = JWT_PRVK.public_key()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash"""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Generate password hash"""
    return pwd_context.hash(password)


def create_access_token(data: dict[str, Any], expires_delta: timedelta | None = None) -> str:
    """Create JWT access token"""
    to_encode = data.copy()
    now = IsoDateTime.now_utc()
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(minutes=jwt_settings.expiration_minutes)

    to_encode.update({"exp": expire})

    encoded_jwt = jwt.encode(to_encode, JWT_PRVK, algorithm=jwt_settings.algorithm)
    return encoded_jwt


def verify_jwt_token(token: str) -> dict[str, Any]:
    """Verify JWT token and return payload"""
    try:

        payload = jwt.decode(token, JWT_PUBK, algorithms=[jwt_settings.algorithm])
        return payload
    except ExpiredSignatureError as e:
        raise UnauthorizedError(message="Token has expired") from e
    except JWTError as e:
        raise UnauthorizedError(message="Invalid token") from e
