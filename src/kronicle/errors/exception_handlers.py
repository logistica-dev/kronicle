# kronicle/errors/exception_handlers.py
import traceback

from fastapi import HTTPException as FastApiHttpException
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHttpException

from kronicle.errors.error_types import KronicleAppError, KronicleHTTPErrorPayload, new_request_id
from kronicle.utils.dev_logs import log_e, log_w


def app_error_handler(request: Request, exc: KronicleAppError) -> JSONResponse:
    """
    Handle KronicleAppError exceptions.
    Logs the error and returns a standardized JSON response.

    Args:
        request: FastAPI request object
        exc: KronicleAppError instance

    Returns:
        JSONResponse with standardized error payload
    """
    log_e(f"KronicleAppError '{exc.status_code}' at {request.method.upper()} {request.url.path}: {exc.detail}")
    return exc.to_error_json(request=request)


def app_error_adapter(request: Request, exc: Exception) -> JSONResponse:
    """
    Adapter to register with FastAPI's add_exception_handler.
    Routes exceptions to app_error_handler if they are KronicleAppError,
    otherwise falls back to the generic handler.

    Args:
        request: FastAPI request object
        exc: Exception instance

    Returns:
        JSONResponse produced by appropriate handler
    """
    return (
        app_error_handler(request, exc)
        if isinstance(exc, KronicleAppError)
        else generic_exception_handler(request, exc)
    )


def fastapi_exception_handler(
    request: Request,
    exc: FastApiHttpException | StarletteHttpException,
) -> JSONResponse:
    """
    Handle FastAPI / Starlette HTTPException.
    Logs warnings and returns a standardized JSON payload.

    Args:
        request: FastAPI request object
        exc: HTTPException instance

    Returns:
        JSONResponse with standardized error payload
    """
    log_w(f"FastApiHttpException '{exc.status_code}' at {request.method.upper()} {request.url.path}: {exc.detail}")
    return KronicleHTTPErrorPayload.from_fastapi_exception(request=request, exc=exc).to_error_json()


def fastapi_exception_adapter(request: Request, exc: Exception) -> JSONResponse:
    """
    Adapter to register with FastAPI's add_exception_handler.
    Routes exceptions to fastapi_exception_handler if they are HTTPExceptions,
    otherwise falls back to the generic handler.

    Args:
        request: FastAPI request object
        exc: Exception instance

    Returns:
        JSONResponse produced by appropriate handler
    """
    return (
        fastapi_exception_handler(request, exc)
        if isinstance(exc, (FastApiHttpException, StarletteHttpException))
        else generic_exception_handler(request, exc)
    )


def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Catch-all handler for unexpected exceptions.
    Logs the full traceback internally and returns a generic InternalServerError payload.

    Args:
        request: FastAPI request object
        exc: Exception instance

    Returns:
        JSONResponse with status=500 and generic InternalServerError info
    """
    request_id = getattr(request.state, "request_id", new_request_id())
    log_e(f"Unhandled exception '{request_id}' at {request.method} {request.url.path}: {exc}")
    log_e(traceback.format_exc())

    return KronicleHTTPErrorPayload.from_exception(
        request=request,
        exc=exc,
        status=500,
        error="InternalServerError",
        message="An unexpected error occurred.",
    ).to_error_json()
