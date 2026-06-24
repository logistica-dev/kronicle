from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import Request, Response

from kronicle.errors.error_types import UnauthorizedError
from kronicle.logging.log_bus.mid_sanitize import RequestSanitizerMiddleware


@pytest.fixture
def mock_app():
    return MagicMock()


@pytest.fixture
def middleware(mock_app):
    return RequestSanitizerMiddleware(mock_app)


@pytest.fixture
def mock_request():
    req = MagicMock(spec=Request)
    req.url = MagicMock()
    req.url.path = "/some/path"
    req.url.__str__ = MagicMock(return_value="http://testserver/some/path")
    req.headers = {}
    return req


@pytest.fixture
def mock_call_next():
    return AsyncMock(return_value=Response("OK"))


class TestInit:
    def test_default_values(self, mock_app):
        mw = RequestSanitizerMiddleware(mock_app)
        assert mw.max_url_length == 2048
        assert mw.max_jwt_length == 4096

    def test_custom_values(self, mock_app):
        mw = RequestSanitizerMiddleware(mock_app, max_url_length=1024, max_jwt_length=2048)
        assert mw.max_url_length == 1024
        assert mw.max_jwt_length == 2048


class TestIsExcludedPath:
    def test_excluded_path_returns_true(self, mock_app):
        mw = RequestSanitizerMiddleware(mock_app)
        assert mw._is_excluded_path("/health/ping") is True
        assert mw._is_excluded_path("/static/style.css") is True
        assert mw._is_excluded_path("/auth/v1/login") is True

    def test_excluded_path_with_docs_public(self, mock_app):
        mw = RequestSanitizerMiddleware(mock_app, are_docs_public=True)
        assert mw._is_excluded_path("/docs") is True
        assert mw._is_excluded_path("/openapi") is True

    def test_non_excluded_path_returns_false(self, middleware):
        assert middleware._is_excluded_path("/api/data") is False


class TestDispatch:
    @pytest.mark.asyncio
    async def test_excluded_path_skips_checks(self, middleware, mock_request, mock_call_next):
        mock_request.url.path = "/health/ping"
        response = await middleware.dispatch(mock_request, mock_call_next)

        assert response.status_code == 200
        mock_call_next.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_url_too_long_raises(self, middleware, mock_request, mock_call_next):
        long_url = "http://testserver/" + "a" * 3000
        mock_request.url.__str__.return_value = long_url

        with pytest.raises(UnauthorizedError, match="URL too long"):
            await middleware.dispatch(mock_request, mock_call_next)

        mock_call_next.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_jwt_too_long_raises(self, middleware, mock_request, mock_call_next):
        mock_request.headers = {"authorization": "Bearer " + "x" * 5000}

        with pytest.raises(UnauthorizedError, match="JWT too long"):
            await middleware.dispatch(mock_request, mock_call_next)

        mock_call_next.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_valid_request_passes(self, middleware, mock_request, mock_call_next):
        mock_request.headers = {"authorization": "Bearer short-token"}

        response = await middleware.dispatch(mock_request, mock_call_next)

        assert response.status_code == 200
        mock_call_next.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_auth_header_still_passes(self, middleware, mock_request, mock_call_next):
        response = await middleware.dispatch(mock_request, mock_call_next)

        assert response.status_code == 200
        mock_call_next.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_sets_secure_headers(self, middleware, mock_request, mock_call_next):
        with patch("kronicle.logging.log_bus.mid_sanitize.secure_headers") as mock_secure:
            mock_secure.set_headers_async = AsyncMock()
            await middleware.dispatch(mock_request, mock_call_next)
            mock_secure.set_headers_async.assert_called_once()
