# kronicle/deps/rbac_deps.py
from __future__ import annotations

from fastapi import Request

from kronicle.auth.auth_service import AuthService
from kronicle.auth.jwt_service import JWTService
from kronicle.services.core_service import CoreService
from kronicle.services.rbac_service import RbacService


def jwt_service(request: Request) -> JWTService:
    return request.app.state.jwt_service


def auth_service(request: Request) -> AuthService:
    return request.app.state.auth_service


def rbac_service(request: Request) -> RbacService:
    return request.app.state.rbac_service


def core_service(request: Request) -> CoreService:
    return request.app.state.core_service
