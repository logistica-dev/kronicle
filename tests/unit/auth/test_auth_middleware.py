# tests/unit/auth/test_auth_middleware.py
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException, Request
from starlette.responses import JSONResponse

from kronicle.auth.auth_middleware import (
    AuthenticationMiddleware,
    ExcludedPaths,
    _check_permission,
    require_auth,
    require_superuser,
)
from kronicle.errors.error_types import ForbiddenError, UnauthorizedError
from kronicle.schemas.permissions.permission import PermStr


class TestExcludedPaths:
    def test_default_excluded_paths(self):
        paths = ExcludedPaths()
        assert paths.is_excluded_path("/")
        assert paths.is_excluded_path("/favicon.ico")
        assert paths.is_excluded_path("/health/live")
        assert paths.is_excluded_path("/static/style.css")
        assert paths.is_excluded_path("/auth/v1/login")
        assert not paths.is_excluded_path("/docs")
        assert not paths.is_excluded_path("/channels")

    def test_docs_are_public(self):
        paths = ExcludedPaths(are_docs_public=True)
        assert paths.is_excluded_path("/docs")
        assert paths.is_excluded_path("/redoc")
        assert paths.is_excluded_path("/openapi.json")
        assert not paths.is_excluded_path("/channels")

    def test_normalize_path_strips_trailing_slash(self):
        assert ExcludedPaths.normalize_path("/channels/") == "/channels"
        assert ExcludedPaths.normalize_path("/channels") == "/channels"
        assert ExcludedPaths.normalize_path("/") == "/"


@pytest.fixture
def jwt_service():
    return MagicMock()


@pytest.fixture
def middleware(jwt_service):
    return AuthenticationMiddleware(app=MagicMock(), jwt_service=jwt_service)


class TestAuthenticationMiddlewareInit:
    def test_raises_when_no_jwt_service(self):
        with pytest.raises(RuntimeError, match="JwtService not initialized"):
            AuthenticationMiddleware(app=MagicMock(), jwt_service=None)


class TestDispatch:
    @pytest.mark.asyncio
    async def test_excluded_path_skips_auth(self, middleware):
        request = MagicMock(spec=Request)
        request.url.path = "/health/live"
        request.headers = {}
        request.state = MagicMock()
        call_next = AsyncMock()
        middleware.is_excluded_path = MagicMock(return_value=True)
        response = await middleware.dispatch(request, call_next)
        assert response == call_next.return_value
        call_next.assert_awaited_once_with(request)

    @patch("kronicle.auth.auth_middleware.app_error_adapter", return_value="error-response")
    @pytest.mark.asyncio
    async def test_missing_header_raises_unauthorized(self, mock_adapter, middleware):
        request = MagicMock(spec=Request)
        request.url.path = "/channels"
        request.headers = {}
        request.app.state.allow_anonymous = False
        request.state = MagicMock()
        response = await middleware.dispatch(request, MagicMock())
        assert response == "error-response"
        mock_adapter.assert_called_once()
        assert isinstance(mock_adapter.call_args[0][1], UnauthorizedError)

    @pytest.mark.asyncio
    async def test_missing_header_allows_anonymous(self, middleware):
        request = MagicMock(spec=Request)
        request.url.path = "/channels"
        request.headers = {}
        request.app.state.allow_anonymous = True
        request.state = MagicMock()
        call_next = AsyncMock()
        await middleware.dispatch(request, call_next)
        assert request.state.user == {"sub": None, "is_anonymous": True, "is_superuser": False}
        assert request.state.authenticated is False
        call_next.assert_awaited_once_with(request)

    @patch("kronicle.auth.auth_middleware.app_error_adapter", return_value="error-response")
    @pytest.mark.asyncio
    async def test_invalid_header_format(self, mock_adapter, middleware):
        request = MagicMock(spec=Request)
        request.url.path = "/channels"
        request.headers = {"Authorization": "Basic xyz"}
        request.state = MagicMock()
        response = await middleware.dispatch(request, MagicMock())
        assert response == "error-response"
        assert isinstance(mock_adapter.call_args[0][1], UnauthorizedError)

    @patch("kronicle.auth.auth_middleware.app_error_adapter", return_value="error-response")
    @pytest.mark.asyncio
    async def test_missing_token(self, mock_adapter, middleware):
        request = MagicMock(spec=Request)
        request.url.path = "/channels"
        request.headers = {"Authorization": "Bearer"}
        request.state = MagicMock()
        response = await middleware.dispatch(request, MagicMock())
        assert response == "error-response"

    @pytest.mark.asyncio
    async def test_valid_token_sets_state(self, middleware, jwt_service):
        request = MagicMock(spec=Request)
        request.url.path = "/channels"
        request.headers = {"Authorization": "Bearer the.token"}
        request.state = MagicMock()
        payload = {"sub": str(uuid4()), "is_superuser": False}
        jwt_service.decode_token.return_value = payload
        call_next = AsyncMock()
        await middleware.dispatch(request, call_next)
        assert request.state.user == payload
        assert request.state.authenticated is True
        jwt_service.decode_token.assert_called_once_with("the.token")
        call_next.assert_awaited_once_with(request)

    @pytest.mark.asyncio
    async def test_invalid_token_returns_json_response(self, middleware, jwt_service):
        request = MagicMock(spec=Request)
        request.url.path = "/channels"
        request.headers = {"Authorization": "Bearer bad.token"}
        request.state = MagicMock()
        jwt_service.decode_token.side_effect = HTTPException(status_code=401, detail="invalid token")
        response = await middleware.dispatch(request, MagicMock())
        assert isinstance(response, JSONResponse)
        assert response.status_code == 401
        assert "invalid token" in bytes(response.body).decode()

    @patch("kronicle.auth.auth_middleware.app_error_adapter", return_value="error-response")
    @pytest.mark.asyncio
    async def test_decode_generic_error_is_unauthorized(self, mock_adapter, middleware, jwt_service):
        request = MagicMock(spec=Request)
        request.url.path = "/channels"
        request.headers = {"Authorization": "Bearer bad.token"}
        request.state = MagicMock()
        jwt_service.decode_token.side_effect = ValueError("malformed")
        response = await middleware.dispatch(request, MagicMock())
        assert response == "error-response"
        assert isinstance(mock_adapter.call_args[0][1], UnauthorizedError)


def test_require_auth_returns_state_user():
    request = MagicMock(spec=Request)
    request.state.user = {"sub": "x"}
    credentials = MagicMock()
    assert require_auth(request=request, credentials=credentials) == {"sub": "x"}


def test_require_auth_returns_anonymous_when_missing():
    request = MagicMock(spec=Request)
    del request.state.user
    credentials = MagicMock()
    assert require_auth(request=request, credentials=credentials) == {
        "sub": None,
        "is_anonymous": True,
        "is_superuser": False,
    }


def test_require_superuser_allows():
    user = {"sub": "x", "is_superuser": True}
    request = MagicMock()
    assert require_superuser(request=request, user=user) == user


def test_require_superuser_forbids():
    request = MagicMock()
    with pytest.raises(ForbiddenError, match="Superuser privileges required"):
        require_superuser(request=request, user={"sub": "x", "is_superuser": False})


def test_check_permission_anonymous_uses_none_sub():
    user = {"sub": None, "is_superuser": False}
    request = MagicMock()
    request.state = MagicMock()
    request.state.__dict__ = {}
    request.app.state.rbac_service.user_has_permission.return_value = True
    _check_permission(request, user, PermStr.ZONE_READ.to_permission())
    request.app.state.rbac_service.user_has_permission.assert_called_once_with(None, PermStr.ZONE_READ.to_permission())


def test_check_permission_superuser_returns_none():
    request = MagicMock()
    request.state = MagicMock()
    request.state.__dict__ = {}
    result = _check_permission(request, {"sub": "x", "is_superuser": True}, PermStr.ZONE_READ.to_permission())
    assert result is None
    request.app.state.rbac_service.user_has_permission.assert_not_called()
