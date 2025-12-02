# kronicle/api/exception_handlers.py

import traceback
from uuid import uuid4

from fastapi import Request
from fastapi.responses import JSONResponse

from kronicle.types.errors import AppError
from kronicle.utils.dev_logs import log_e


# ---------------------------
# Helper to generate request ID
# ---------------------------
def new_request_id() -> str:
    return str(uuid4())


async def app_error_handler(request: Request, exc: Exception):
    request_id = getattr(request.state, "request_id", new_request_id())

    if not isinstance(exc, AppError):
        # Fallback: treat unknown exceptions as 500
        return JSONResponse(
            status_code=500,
            content={
                "status": 500,
                "error": "InternalServerError",
                "message": str(exc),
                "details": {},
                "path": request.url.path,
                "method": request.method,
                "request_id": request_id,
            },
        )

    # Handle AppError
    return JSONResponse(
        status_code=exc.status,
        content={
            **exc.to_dict(),
            "path": request.url.path,
            "method": request.method,
            "request_id": request_id,
        },
    )


async def generic_exception_handler(request: Request, exc: Exception):
    """
    Catch unexpected exceptions and convert to JSON response.
    Logs error internally without exposing stack traces to the client.
    """
    request_id = getattr(request.state, "request_id", new_request_id())
    log_e(f"Unhandled exception at {request.method} {request.url.path}: {exc}")
    log_e(traceback.format_exc())

    return JSONResponse(
        status_code=500,
        content={
            "status": 500,
            "error": "InternalServerError",
            "message": "An unexpected error occurred.",
            "details": {},
            "path": request.url.path,
            "method": request.method,
            "request_id": request_id,
        },
    )
