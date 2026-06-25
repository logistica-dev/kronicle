# tests/unit/errors/test_error_types.py
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ValidationError

from kronicle.errors.error_types import (
    AppStartupError,
    BadRequestError,
    ConflictError,
    DatabaseConnectionError,
    DatabaseInstructionError,
    ForbiddenError,
    KronicleAppError,
    KronicleHTTPErrorPayload,
    NotFoundError,
    UnauthorizedError,
    kronicle_app_error_handler,
    new_request_id,
)


@pytest.fixture
def mock_request():
    req = MagicMock(spec=Request)
    req.url.path = "/test/path"
    req.method = "POST"
    req.state.request_id = "req-123"
    return req


class TestNewRequestId:
    def test_returns_uuid4_string(self):
        rid = new_request_id()
        assert isinstance(rid, str)
        parts = rid.split("-")
        assert len(parts) == 5


class TestKronicleHTTPErrorPayload:
    def test_from_app_exception(self, mock_request):
        exc = BadRequestError("Something went wrong", details={"field": "value"})
        payload = KronicleHTTPErrorPayload.from_app_exception(exc, mock_request)

        assert payload.status == 400
        assert payload.error == "BadRequest"
        assert payload.message == "Something went wrong"
        assert payload.details == {"field": "value"}
        assert payload.path == "/test/path"
        assert payload.method == "POST"
        assert payload.request_id == "req-123"

    def test_from_app_exception_generates_request_id(self, mock_request):
        del mock_request.state.request_id
        exc = BadRequestError("msg")
        payload = KronicleHTTPErrorPayload.from_app_exception(exc, mock_request)

        assert payload.request_id != "req-123"
        assert isinstance(payload.request_id, str)

    def test_from_fastapi_exception(self, mock_request):

        exc = HTTPException(status_code=404, detail="Not found")
        payload = KronicleHTTPErrorPayload.from_fastapi_exception(mock_request, exc)

        assert payload.status == 404
        assert payload.error == "HTTPError"
        assert payload.message == "Not found"

    def test_from_pydantic_core_validation(self, mock_request):
        try:

            class TestModel(BaseModel):
                x: int

            TestModel(x="not_a_number")  # type: ignore
        except ValidationError as e:
            payload = KronicleHTTPErrorPayload.from_pydantic_core_validation(mock_request, e)

            assert payload.status == 422
            assert payload.error == "ValidationError"
            assert payload.details is not None

    def test_from_pydantic_validation(self, mock_request):

        exc = RequestValidationError(errors=[{"loc": ("body", "name"), "msg": "field required", "type": "value_error"}])
        payload = KronicleHTTPErrorPayload.from_pydantic_validation(mock_request, exc)

        assert payload.status == 422
        assert payload.error == "ValidationError"
        assert payload.details
        assert "body.name" in payload.details

    def test_from_exception_with_explicit_args(self, mock_request):
        exc = ValueError("original error")
        payload = KronicleHTTPErrorPayload.from_exception(
            request=mock_request,
            exc=exc,
            status=500,
            error="InternalServerError",
            message="An unexpected error occurred.",
            details={"reason": "unknown"},
        )

        assert payload.status == 500
        assert payload.error == "InternalServerError"
        assert payload.message == "An unexpected error occurred."
        assert payload.details == {"reason": "unknown"}

    def test_from_exception_falls_back_to_exception_attrs(self, mock_request):
        exc = BadRequestError("custom message", details={"x": 1})
        payload = KronicleHTTPErrorPayload.from_exception(request=mock_request, exc=exc)

        assert payload.status == 400
        assert payload.error == "Error"
        assert payload.details == {"x": 1}
        assert "custom message" in payload.message

    def test_to_error_json_filters_empty_values(self, mock_request):
        exc = BadRequestError("msg")
        payload = KronicleHTTPErrorPayload.from_app_exception(exc, mock_request)

        json_resp = payload.to_error_json()
        body = bytes(json_resp.body).decode()

        assert json_resp.status_code == 400
        assert "status" in body
        assert "error" in body
        assert "message" in body


class TestKronicleAppError:
    def test_init_sets_attributes(self):
        exc = KronicleAppError(status=400, error="TestError", message="test msg", details={"k": "v"})
        assert exc.status == 400
        assert exc.error == "TestError"
        assert exc.message == "test msg"
        assert exc.details == {"k": "v"}

    def test_to_dict(self):
        exc = BadRequestError("msg")
        d = exc.to_dict()
        assert d["status"] == 400
        assert d["error"] == "BadRequest"
        assert d["message"] == "msg"

    def test_to_http_model(self, mock_request):
        exc = NotFoundError("not found")
        model = exc.to_http_model(mock_request)
        assert isinstance(model, KronicleHTTPErrorPayload)
        assert model.status == 404

    def test_to_error_json(self, mock_request):
        exc = ConflictError("conflict")
        response = exc.to_error_json(mock_request)
        assert response.status_code == 409


class TestConcreteErrors:
    def test_bad_request(self):
        exc = BadRequestError("bad", details={"field": "x"})
        assert exc.status == 400
        assert exc.error == "BadRequest"

    def test_unauthorized(self):
        exc = UnauthorizedError("unauth")
        assert exc.status == 401
        assert exc.error == "Unauthorized"

    def test_forbidden(self):
        exc = ForbiddenError("forbidden")
        assert exc.status == 403
        assert exc.error == "Forbidden"

    def test_not_found(self):
        exc = NotFoundError("missing")
        assert exc.status == 404
        assert exc.error == "NotFound"

    def test_conflict(self):
        exc = ConflictError("conflict")
        assert exc.status == 409
        assert exc.error == "Conflict"

    def test_app_startup_error(self):
        exc = AppStartupError("startup failed")
        assert exc.status == 500
        assert exc.error == "AppStartupError"

    def test_db_connection_error(self):
        exc = DatabaseConnectionError("db down")
        assert exc.status == 502
        assert exc.error == "DatabaseConnectionError"

    def test_db_instruction_error(self):
        exc = DatabaseInstructionError("query failed")
        assert exc.status == 500
        assert exc.error == "DatabaseInstructionError"


class TestKronicleAppErrorHandler:
    @pytest.mark.asyncio
    async def test_returns_json_response(self, mock_request):
        exc = BadRequestError("bad request")
        response = await kronicle_app_error_handler(mock_request, exc)
        assert response.status_code == 400
        assert response.body
        assert "bad request" in bytes(response.body).decode()
