# kronicle/errors/error_types.py
from __future__ import annotations

from typing import Any

from fastapi import HTTPException, Request, status
from fastapi import HTTPException as FastApiHttpException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError
from starlette.exceptions import HTTPException as StarletteHttpException

from kronicle.utils.str_utils import uuid4_str


# ------------------------------------------------------------------------------
# Helper to generate request ID
# ------------------------------------------------------------------------------
def new_request_id() -> str:
    """
    Generate a new UUID4 string to uniquely identify a request.
    Useful for correlating logs and error responses.
    """
    return uuid4_str()


# ------------------------------------------------------------------------------
# Standardized error payload
# ------------------------------------------------------------------------------
class KronicleHTTPErrorPayload(BaseModel):
    """
    Schema for returning all HTTP errors to clients in a consistent format.
    Used for both expected (KronicleAppError, HTTPException) and unexpected exceptions.
    """

    status: int
    error: str
    message: str
    details: dict[str, Any] | None = Field(default=None)
    path: str
    method: str
    request_id: str

    @classmethod
    def from_app_exception(cls, exc: KronicleAppError, request: Request) -> KronicleHTTPErrorPayload:
        """
        Convert a KronicleAppError into the full HTTP error model including request context.
        """
        request_id = getattr(request.state, "request_id", new_request_id())
        return cls(
            status=exc.status,
            error=exc.error,
            message=exc.message,
            details=exc.details if isinstance(exc.details, dict) else None,
            path=request.url.path,
            method=request.method.upper() if isinstance(request.method, str) else request.method,
            request_id=request_id,
        )

    @classmethod
    def from_fastapi_exception(
        cls,
        request: Request,
        exc: FastApiHttpException | StarletteHttpException,
    ) -> KronicleHTTPErrorPayload:
        """
        Build a standardized error payload from a FastAPI or Starlette HTTPException.

        Args:
            request: The FastAPI request object
            exc: HTTPException instance

        Returns:
            KronicleHTTPErrorPayload instance
        """
        request_id = getattr(request.state, "request_id", new_request_id())
        return cls(
            status=exc.status_code,
            error="HTTPError",
            message=str(exc.detail),
            path=request.url.path,
            method=request.method.upper() if isinstance(request.method, str) else request.method,
            request_id=request_id,
        )

    @classmethod
    def from_pydantic_core_validation(
        cls,
        request: Request,
        exc: ValidationError,
        status: int = 422,
        error: str = "ValidationError",
        message: str = "Request validation failed",
    ) -> KronicleHTTPErrorPayload:
        """
        Build a KronicleHTTPErrorPayload from a pydantic_core.ValidationError (Pydantic v2).

        Args:
            request: FastAPI request object
            exc: pydantic_core.ValidationError instance
            status: HTTP status code (default 422)
            error: Short error string (default "ValidationError")
            message: Human-readable message (default "Request validation failed")

        Returns:
            KronicleHTTPErrorPayload instance
        """
        request_id = getattr(request.state, "request_id", new_request_id())

        # exc.errors() returns a list of dicts with 'type', 'loc', 'msg'
        details: dict[str, Any] = {}
        for err in exc.errors():
            loc = ".".join(str(l) for l in err.get("loc", [])) or "unknown"
            msg = err.get("msg", "")
            typ = err.get("type", "")
            details[loc] = {"message": msg, "type": typ}

        return cls(
            status=status,
            error=error,
            message=message,
            details=details,
            path=request.url.path,
            method=request.method.upper() if isinstance(request.method, str) else request.method,
            request_id=request_id,
        )

    @classmethod
    def from_pydantic_validation(
        cls,
        request: Request,
        exc: RequestValidationError,
        status: int = 422,
        error: str = "ValidationError",
        message: str = "Request validation failed",
    ) -> KronicleHTTPErrorPayload:
        """
        Build a KronicleHTTPErrorPayload from a Pydantic validation error.

        Args:
            request: FastAPI request object
            exc: RequestValidationError instance
            status: HTTP status code (default 422)
            error: Short error string (default "ValidationError")
            message: Human-readable message (default "Request validation failed")

        Returns:
            KronicleHTTPErrorPayload instance
        """
        request_id = getattr(request.state, "request_id", new_request_id())
        details: dict[str, list[dict[str, str]]] = {}

        # Convert exc.errors() list into a dict of field -> error messages

        for err in exc.errors():
            loc = ".".join(str(l) for l in err.get("loc", [])) or "unknown"
            msg = err.get("msg", "")
            typ = err.get("type", "")
            details.setdefault(loc, []).append({"message": msg, "type": typ})

        return cls(
            status=status,
            error=error,
            message=message,
            details=details,
            path=request.url.path,
            method=request.method.upper() if isinstance(request.method, str) else request.method,
            request_id=request_id,
        )

    @classmethod
    def from_exception(
        cls,
        *,
        request: Request,
        exc: Exception,
        status: int | None = None,
        error: str | None = None,
        message: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> KronicleHTTPErrorPayload:
        """
        Create a KronicleHTTPErrorPayload from an exception and request.

        Args:
            request: The FastAPI request object
            exc: The exception to serialize
            status: Optional HTTP status code override
            error: Optional short error string override
            message: Optional detailed message override
            details: Optional dictionary of extra details

        Returns:
            KronicleHTTPErrorPayload instance
        """
        request_id = getattr(request.state, "request_id", new_request_id())
        exc_status = status or getattr(exc, "status", 500)
        exc_message = message or getattr(exc, "message", str(exc))
        exc_details = details or getattr(exc, "details", {}) or getattr(exc, "detail", {})
        return KronicleHTTPErrorPayload(
            status=exc_status if isinstance(exc_status, int) else 500,
            error=error if isinstance(error, str) else "Error",
            message=exc_message if isinstance(exc_message, str) else str(exc_message),
            details=exc_details if isinstance(exc_details, dict) else None,
            path=request.url.path,
            method=request.method.upper() if isinstance(request.method, str) else request.method,
            request_id=request_id,
        )

    def to_error_json(self) -> JSONResponse:
        """
        Convert the error model into a FastAPI JSONResponse
        with the correct HTTP status code.
        """
        # return JSONResponse(status_code=self.status, content=self.model_dump())
        data = self.model_dump()
        filtered = {k: v for k, v in data.items() if v not in (None, {}, [], "")}
        return JSONResponse(status_code=self.status, content=filtered)


class KronicleAppError(HTTPException):
    """
    Base application error with structured JSON output.
    """

    def __init__(self, status: int, error: str, message: str, details: dict[str, Any] | None = None):
        super().__init__(
            status_code=status,
            detail={
                "message": message,
                "error": error,
                "details": details or None,
            },
        )
        self.status = status
        self.error = error
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict:
        """
        Convert error into a JSON-serializable dictionary.
        """
        return {
            "status": self.status,
            "error": self.error,
            "message": self.message,
            "details": self.details,  # e.g. {"channel_id": "Provide a valid channel_id"}
        }

    def to_http_model(self, request: Request) -> KronicleHTTPErrorPayload:
        """Convert KronicleAppError into full HTTP response model including request context"""
        return KronicleHTTPErrorPayload.from_app_exception(request=request, exc=self)

    def to_error_json(self, request: Request) -> JSONResponse:
        return self.to_http_model(request=request).to_error_json()


class BadRequestError(KronicleAppError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(status=status.HTTP_400_BAD_REQUEST, error="BadRequest", message=message, details=details)


class UnauthorizedError(KronicleAppError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(status=status.HTTP_401_UNAUTHORIZED, error="Unauthorized", message=message, details=details)


class NotFoundError(KronicleAppError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(status=status.HTTP_404_NOT_FOUND, error="NotFound", message=message, details=details)


class ConflictError(KronicleAppError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(status=status.HTTP_409_CONFLICT, error="Conflict", message=message, details=details)


class AppStartupError(KronicleAppError):
    """
    Raised when the application cannot start properly,
    e.g., database connection failed after retries.
    """

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(
            status=status.HTTP_500_INTERNAL_SERVER_ERROR, error="AppStartupError", message=message, details=details
        )


class DatabaseConnectionError(KronicleAppError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(
            status=status.HTTP_502_BAD_GATEWAY, error="DatabaseConnectionError", message=message, details=details
        )


class DatabaseInstructionError(KronicleAppError):
    def __init__(self, message: str = "Unexpected database error", details: dict[str, Any] | None = None):
        super().__init__(
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error="DatabaseInstructionError",
            message=message,
            details=details,
        )


async def kronicle_app_error_handler(request: Request, exc: KronicleAppError):
    """
    Global handler for all KronicleAppError exceptions.
    Converts them into KronicleHTTPErrorPayload and returns JSONResponse.
    """
    return exc.to_http_model(request=request).to_error_json()
