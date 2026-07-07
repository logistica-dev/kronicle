# kronicle/auth/auth_middleware.py
"""
Authentication middleware for FastAPI
"""

from typing import Callable
from uuid import UUID

from fastapi import Depends, HTTPException, Request, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from kronicle.auth.jwt_service import JWTService
from kronicle.errors.error_types import ForbiddenError, KronicleAppError, UnauthorizedError
from kronicle.errors.exception_handlers import app_error_adapter
from kronicle.schemas.permissions.permission import Permission, PermStr
from kronicle.utils.dev_logs import log_d


class ExcludedPaths:
    # Routes that don't require authentication
    EXCLUDED_PATHS = {
        "/",
        "/favicon.ico",
    }
    EXCLUDED_PREFIXES = (
        "/static/",
        "/health/",
        "/auth/v1/",
    )
    DOCS_PREFIXES = (
        "/docs",
        "/redoc",
        "/static-docs",
        "/openapi",
    )

    def __init__(self, are_docs_public: bool = False):
        if are_docs_public:
            self.EXCLUDED_PREFIXES += self.DOCS_PREFIXES

    @classmethod
    def normalize_path(cls, path: str) -> str:
        return path.rstrip("/")

    def is_excluded_path(self, path: str) -> bool:
        """Check if path is excluded from authentication"""
        # Normalize trailing slash
        normalized_path = self.normalize_path(path)

        # Exact matches
        return normalized_path in self.EXCLUDED_PATHS or any(
            normalized_path.startswith(prefix) for prefix in self.EXCLUDED_PREFIXES
        )


class AuthenticationMiddleware(BaseHTTPMiddleware):
    """Middleware to handle JWT authentication for protected routes"""

    def __init__(self, app, jwt_service: JWTService, are_docs_public: bool = False):
        if jwt_service is None:
            raise RuntimeError("[AuthService] JwtService not initialized. Call init() from main app first.")

        super().__init__(app)
        self._jwt_service = jwt_service

        self._safe_paths = ExcludedPaths(are_docs_public)

        log_d("auth.init", f"Docs are {'' if are_docs_public else 'not '}public")

        log_d("auth.init", "Authorized paths", self._safe_paths.EXCLUDED_PREFIXES)

    def is_excluded_path(self, path: str) -> bool:
        """Check if path is excluded from authentication"""
        # Normalize trailing slash
        return self._safe_paths.is_excluded_path(path)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        try:
            # Check if path requires authentication
            if self.is_excluded_path(request.url.path):
                return await call_next(request)

            # Extract Authorization header
            auth_header = request.headers.get("Authorization")
            if not auth_header:
                raise UnauthorizedError(message="Authorization header missing")

            # Validate Bearer token format
            if not auth_header.startswith("Bearer "):
                raise UnauthorizedError("Invalid authorization header format")

            # Extract token
            token = auth_header.split(" ")[1] if len(auth_header.split(" ")) == 2 else None
            if not token:
                raise UnauthorizedError("Token missing")

            # Verify JWT token
            try:
                payload = self._jwt_service.decode_token(token)
                # Add user information to request state
                request.state.user = payload
                request.state.authenticated = True
            except HTTPException as exc:
                return JSONResponse(
                    status_code=exc.status_code, content={"detail": exc.detail}, headers=exc.headers or {}
                )
            except Exception as exc:
                raise UnauthorizedError("Invalid authentication credentials") from exc

            # Continue with request processing
            response = await call_next(request)
            return response
        except KronicleAppError as exc:
            return app_error_adapter(request, exc)


def get_current_user_from_request(request: Request) -> dict:
    """Get current user from request state (for use in route handlers)"""
    if hasattr(request.state, "user") and request.state.user:
        return request.state.user
    raise UnauthorizedError(message="User not authenticated")


bearer_scheme = HTTPBearer(auto_error=False)


def require_auth(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),  # noqa: B008
) -> dict:
    """
    Dependency for JWT-protected routes.
    - Swagger shows a single field for JWT.
    - Middleware still validates the token.
    """
    return get_current_user_from_request(request)


def require_superuser(
    request: Request,
    user: dict = Depends(require_auth),  # noqa: B008
) -> dict:
    """
    Dependency that requires the current user to be a superuser.
    Must be used AFTER require_auth (or on a router that already has require_auth).
    """
    if not user.get("is_superuser"):
        raise ForbiddenError("Superuser privileges required")
    return user


def _check_permission(request: Request, user: dict, perm_obj: Permission) -> bool | None:
    """Check a single permission with caching. Returns True/False/None (superuser)."""
    if user.get("is_superuser"):
        return None

    perm_str = str(perm_obj)
    cache: dict = request.state.__dict__.setdefault("_perm_cache", {})
    if perm_str in cache:
        return cache[perm_str]

    rbac = request.app.state.rbac_service
    has_perm = rbac.user_has_permission(UUID(user["sub"]), perm_obj)
    cache[perm_str] = has_perm
    return has_perm


def require_permission(permission: PermStr | str | Permission):
    """
    Factory that returns a dependency which checks if the authenticated user
    has a specific permission via the RBAC policy engine.

    Superuser flag in JWT bypasses all permission checks.

    Usage:
        @router.get("/admin", dependencies=[Depends(require_permission(PermStr.RBAC_ACCESS_PROFILE))])
        def admin_endpoint(): ...
    """

    if isinstance(permission, PermStr):
        perm_obj = permission.to_permission()
    elif isinstance(permission, str):
        perm_obj = Permission.parse(permission)
    else:
        perm_obj = permission

    def _require_permission(
        request: Request,
        user: dict = Depends(require_auth),  # noqa: B008
    ) -> dict:
        result = _check_permission(request, user, perm_obj)
        if result is None:
            return user
        if not result:
            raise ForbiddenError(f"Missing required permission: '{perm_obj}'")
        return user

    return _require_permission


def require_permission_set(*permissions: PermStr | str | Permission):
    """
    Factory returning a dependency that passes if the user has ALL specified permissions (AND).

    Usage:
        @router.post("/clone", dependencies=[Depends(require_permission_set(PermStr.CHANNEL_READ, PermStr.CHANNEL_CREATE))])
        def clone_endpoint(): ...
    """

    perm_objects = [Permission.parse(p) if isinstance(p, str) else p for p in permissions]

    def _require_set(
        request: Request,
        user: dict = Depends(require_auth),  # noqa: B008
    ) -> dict:
        for perm in perm_objects:
            result = _check_permission(request, user, perm)
            if result is None:
                return user
            if not result:
                raise ForbiddenError(f"Missing required permission: '{perm}'")
        return user

    return _require_set


def require_any_permission(*permissions: PermStr | str | Permission):
    """
    Factory returning a dependency that passes if the user has ANY of the specified permissions (OR).

    Usage:
        @router.get("/rows", dependencies=[Depends(require_any_permission(PermStr.CHANNEL_READ, PermStr.ROW_READ))])
        def rows_endpoint(): ...
    """

    perm_objects = [Permission.parse(p) if isinstance(p, str) else p for p in permissions]

    def _require_any(
        request: Request,
        user: dict = Depends(require_auth),  # noqa: B008
    ) -> dict:
        for perm in perm_objects:
            result = _check_permission(request, user, perm)
            if result is None:
                return user
            if result:
                return user
        names = ", ".join(str(p) for p in perm_objects)
        raise ForbiddenError(f"Missing required permission: need one of ({names})")

    return _require_any
