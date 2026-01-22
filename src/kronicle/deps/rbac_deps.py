# kronicle/deps/rbac_deps.py
from fastapi import Request

from kronicle.auth.auth_service import AuthService
from kronicle.auth.jwt_service import JWTService
from kronicle.services.rbac_service import RbacService


def jwt_service(request: Request) -> JWTService:
    return request.app.state.jwt_service


def auth_service(request: Request) -> AuthService:
    return request.app.state.auth_service


def rbac_service(request: Request) -> RbacService:
    return request.app.state.rbac_service
