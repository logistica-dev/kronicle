# kronicle/api/health_check.py

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from kronicle._build import __build_date__, __commit__
from kronicle.deps.channel_deps import channel_service
from kronicle.deps.settings_ini import package_version
from kronicle.services.channel_service import ChannelService
from kronicle.types.iso_datetime import IsoDateTime

health_check = APIRouter(tags=["Check health"])


@health_check.get("/live", include_in_schema=True)
def liveness():
    return {"status": "alive"}


@health_check.get("/ready", include_in_schema=True)
async def readiness(
    data_service: ChannelService = Depends(channel_service),  # noqa: B008
):
    try:
        # Minimal DB probe
        is_ready: bool = await data_service.ping()  # type: ignore[attr-defined]
        if is_ready:
            return JSONResponse({"status": "ready"}, status_code=200)
        return JSONResponse({"status": "not_ready"}, status_code=503)
    except Exception as e:
        return JSONResponse({"status": "not_ready", "error": str(e)}, status_code=503)


@health_check.get("/version", include_in_schema=True)
def version():
    return {"version": package_version(), "commit": __commit__, "date": IsoDateTime(__build_date__)}
