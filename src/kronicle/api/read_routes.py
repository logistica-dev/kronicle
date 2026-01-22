# kronicle/api/read_routes.py

from fastapi import APIRouter, Depends

from kronicle.api.shared_read_routes import shared_read_router
from kronicle.auth.auth_middleware import require_auth

"""
Routes available to users with read-only permissions.
These endpoints allow safe retrieval of channel metadata and stored data.
"""
reader_router = APIRouter(tags=["Read data"], dependencies=[Depends(require_auth)])

reader_router.include_router(shared_read_router)
