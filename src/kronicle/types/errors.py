# kronicle/types/errors.py
from typing import Any

from fastapi import HTTPException, status


class AppError(HTTPException):
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
            "details": self.details,  # e.g. {"sensor_id": "Provide a valid sensor_id"}
        }


class BadRequestError(AppError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(status.HTTP_400_BAD_REQUEST, "BadRequest", message, details)


class UnauthorizedError(AppError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(status.HTTP_401_UNAUTHORIZED, "Unauthorized", message, details)


class NotFoundError(AppError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(status.HTTP_404_NOT_FOUND, "NotFound", message, details)


class ConflictError(AppError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(status.HTTP_409_CONFLICT, "Conflict", message, details)


class AppStartupError(AppError):
    """
    Raised when the application cannot start properly,
    e.g., database connection failed after retries.
    """

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(status.HTTP_500_INTERNAL_SERVER_ERROR, "AppStartupError", message, details)


class DatabaseConnectionError(AppError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(status.HTTP_502_BAD_GATEWAY, "DatabaseConnectionError", message, details)


class DatabaseInstructionError(AppError):
    def __init__(self, message: str = "Unexpected database error", details: dict[str, Any] | None = None):
        super().__init__(status.HTTP_500_INTERNAL_SERVER_ERROR, "DatabaseInstructionError", message, details)
