"""
Authentication middleware for FastAPI
"""

from typing import Callable

from fastapi import Request, Response
from secure import Secure
from starlette.middleware.base import BaseHTTPMiddleware

from kronicle.auth.auth_middleware import ExcludedPaths
from kronicle.errors.error_types import UnauthorizedError

secure_headers = Secure.with_default_headers()


class RequestSanitizerMiddleware(BaseHTTPMiddleware):
    """
    Middleware to sanitize incoming requests
    """

    def __init__(self, app, max_url_length: int = 2048, max_jwt_length: int = 4096, are_docs_public: bool = False):
        super().__init__(app)
        self.max_url_length = max_url_length
        self.max_jwt_length = max_jwt_length

        self._safe_paths = ExcludedPaths(are_docs_public)

    def _is_excluded_path(self, path: str) -> bool:
        """Check if path is excluded from authentication"""
        # Exact match
        return self._safe_paths.is_excluded_path(path)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # here = "dispatch"
        # Check if path can be excluded
        if self._is_excluded_path(request.url.path):
            return await call_next(request)

        # Check URL length
        url_length = len(str(request.url))
        if url_length > self.max_url_length:
            raise UnauthorizedError(message=f"URL too long: {url_length} chars")

        # Check JWT length in Authorization header
        auth = request.headers.get("authorization")
        if auth and auth.lower().startswith("bearer "):
            token = auth[7:]
            if len(token) > self.max_jwt_length:
                raise UnauthorizedError(message="JWT too long")

        response = await call_next(request)
        await secure_headers.set_headers_async(response)
        return response
