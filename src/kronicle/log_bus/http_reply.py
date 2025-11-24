from fastapi import HTTPException, status
from starlette.responses import JSONResponse


class UnauthorizedResponse(JSONResponse):
    def __init__(self, detail) -> None:
        super().__init__(
            content={"detail": detail},
            status_code=status.HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": "Bearer"},
        )


class UnauthorizedException(HTTPException):
    def __init__(self, detail) -> None:
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail,
            headers={"WWW-Authenticate": "Bearer"},
        )
