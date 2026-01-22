# kronicle/auth/auth_middleware.py
"""
Authentication middleware for FastAPI
"""

from typing import Callable

from fastapi import Depends, HTTPException, Request, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from kronicle.auth.jwt_service import JWTService
from kronicle.errors.error_types import KronicleAppError, UnauthorizedError
from kronicle.errors.exception_handlers import app_error_adapter
from kronicle.utils.dev_logs import log_d


class AuthenticationMiddleware(BaseHTTPMiddleware):
    """Middleware to handle JWT authentication for protected routes"""

    # Routes that don't require authentication
    _EXCLUDED_PATHS = {
        "/",
    }
    _EXCLUDED_PREFIXES = (
        "/static/",
        "/health/",
        "/auth/v1/",
    )
    _DOCS_PREFIXES = (
        "/docs",
        "/openapi",
    )

    def __init__(self, app, jwt_service: JWTService, are_docs_public: bool = False):
        if jwt_service is None:
            raise RuntimeError("[AuthService] JwtService not initialized. Call init() from main app first.")

        super().__init__(app)
        self._jwt_service = jwt_service

        if are_docs_public:
            self._EXCLUDED_PREFIXES += self._DOCS_PREFIXES

        log_d("auth.init", f"Docs are {'' if are_docs_public else 'not '}public")
        log_d("auth.init", "Authorized paths", self._EXCLUDED_PREFIXES)

    def _normalize_path(self, path: str) -> str:
        return path.rstrip("/")

    def _is_excluded_path(self, path: str) -> bool:
        """Check if path is excluded from authentication"""
        # Normalize trailing slash
        normalized_path = path.rstrip("/")

        # Exact matches
        if normalized_path in self._EXCLUDED_PATHS:
            return True

        # Prefix matches
        return any(normalized_path.startswith(prefix) for prefix in self._EXCLUDED_PREFIXES)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        try:
            # Check if path requires authentication
            if self._is_excluded_path(request.url.path):
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
