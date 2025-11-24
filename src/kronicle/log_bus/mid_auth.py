"""
Authentication middleware for FastAPI
"""

from typing import Callable

from fastapi import HTTPException, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from kronicle.core.security import verify_jwt_token
from kronicle.types.errors import UnauthorizedError


class AuthenticationMiddleware(BaseHTTPMiddleware):
    """Middleware to handle JWT authentication for protected routes"""

    # Routes that don't require authentication
    EXCLUDED_PATHS = {
        "/",
        "/health",
        "/docs",
        "/redoc",
        "/openapi",
        "/openapi.json",
        "/api/v1/auth/token",
        "/api/v1/auth/validate",
    }

    def __init__(self, app):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
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
            payload = verify_jwt_token(token)
            # Add user information to request state
            request.state.user = payload
            request.state.authenticated = True
        except HTTPException as e:
            return JSONResponse(status_code=e.status_code, content={"detail": e.detail}, headers=e.headers or {})
        except Exception as e:
            raise UnauthorizedError("Invalid authentication credentials") from e

        # Continue with request processing
        response = await call_next(request)
        return response

    def _is_excluded_path(self, path: str) -> bool:
        """Check if path is excluded from authentication"""
        # Exact match
        if path in self.EXCLUDED_PATHS:
            return True

        # Pattern match for static files and docs
        if path.startswith("/static/") or path.startswith("/docs") or path.startswith("/redoc"):
            return True

        return False


def get_current_user_from_request(request: Request) -> dict:
    """Get current user from request state (for use in route handlers)"""
    if hasattr(request.state, "user") and request.state.user:
        return request.state.user
    raise UnauthorizedError(message="User not authenticated")


def require_auth(request: Request) -> dict:
    """Dependency function to get authenticated user (simplified replacement for Depends)"""
    return get_current_user_from_request(request)
