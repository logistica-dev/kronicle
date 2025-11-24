# kronicle/routes/read_routes.py

from fastapi import APIRouter

from kronicle.api.routes.shared_read_routes import shared_read_router

"""
Routes available to users with read-only permissions.
These endpoints allow safe retrieval of sensor metadata and stored data.
"""
reader_router = APIRouter(tags=["Read data"])

reader_router.include_router(shared_read_router)
