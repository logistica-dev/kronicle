# tests/unit/errors/test_exception_handlers.py
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError

from kronicle.errors.error_types import BadRequestError
from kronicle.errors.exception_handlers import (
    app_error_adapter,
    app_error_handler,
    fastapi_exception_adapter,
    fastapi_exception_handler,
    generic_exception_handler,
    pydantic_exception_adapter,
    pydantic_exception_handler,
)


@pytest.fixture
def mock_request():
    req = MagicMock(spec=Request)
    req.url.path = "/test/path"
    req.method = "POST"
    req.state.request_id = str(uuid4())
    return req


class TestAppErrorHandler:
    def test_returns_json_response(self, mock_request):
        exc = BadRequestError("bad request")
        response = app_error_handler(mock_request, exc)

        assert response.status_code == 400
        body = bytes(response.body).decode()
        assert "bad request" in body

    def test_logs_error(self, mock_request):
        exc = BadRequestError("bad request")
        with patch("kronicle.errors.exception_handlers.log_e") as mock_log:
            app_error_handler(mock_request, exc)
            mock_log.assert_called_once()


class TestAppErrorAdapter:
    def test_routes_kronicle_error_to_app_handler(self, mock_request):
        exc = BadRequestError("test")
        response = app_error_adapter(mock_request, exc)
        assert response.status_code == 400

    def test_routes_other_exceptions_to_generic(self, mock_request):
        exc = ValueError("generic")
        response = app_error_adapter(mock_request, exc)
        assert response.status_code == 500


class TestFastapiExceptionHandler:
    def test_returns_json_response(self, mock_request):
        exc = HTTPException(status_code=404, detail="Not found")
        response = fastapi_exception_handler(mock_request, exc)

        assert response.status_code == 404
        body = bytes(response.body).decode()
        assert "Not found" in body


class TestFastapiExceptionAdapter:
    def test_routes_http_exception(self, mock_request):
        exc = HTTPException(status_code=403, detail="Forbidden")
        response = fastapi_exception_adapter(mock_request, exc)
        assert response.status_code == 403

    def test_routes_other_exceptions_to_generic(self, mock_request):
        exc = ValueError("generic")
        response = fastapi_exception_adapter(mock_request, exc)
        assert response.status_code == 500


class TestPydanticExceptionHandler:
    def test_handles_request_validation_error(self, mock_request):
        exc = RequestValidationError(errors=[{"loc": ("body",), "msg": "invalid", "type": "value_error"}])
        response = pydantic_exception_handler(mock_request, exc)
        assert response.status_code == 422

    def test_handles_validation_error(self, mock_request):
        from pydantic import BaseModel, ValidationError

        class M(BaseModel):
            x: int

        try:
            M(x="bad")  # type: ignore
        except ValidationError as e:
            exc = e

            response = pydantic_exception_handler(mock_request, exc)
            assert response.status_code == 422


class TestPydanticExceptionAdapter:
    def test_routes_validation_error(self, mock_request):
        from pydantic import BaseModel, ValidationError

        class M(BaseModel):
            x: int

        try:
            M(x="bad")  # type: ignore
        except ValidationError as e:
            exc = e

            response = pydantic_exception_adapter(mock_request, exc)
            assert response.status_code == 422

    def test_routes_other_exceptions_to_generic(self, mock_request):
        exc = ValueError("generic")
        response = pydantic_exception_adapter(mock_request, exc)
        assert response.status_code == 500


class TestGenericExceptionHandler:
    def test_returns_500_json_response(self, mock_request):
        exc = ValueError("something broke")
        response = generic_exception_handler(mock_request, exc)

        assert response.status_code == 500
        body = bytes(response.body).decode()
        assert "InternalServerError" in body

    def test_logs_error(self, mock_request):
        exc = ValueError("something broke")
        with patch("kronicle.errors.exception_handlers.log_e") as mock_log:
            generic_exception_handler(mock_request, exc)
            assert mock_log.called
