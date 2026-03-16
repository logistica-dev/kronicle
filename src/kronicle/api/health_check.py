# kronicle/api/health_check.py

from fastapi import APIRouter, Depends

from kronicle._build import __build_date__, __commit__, __version__
from kronicle.deps.channel_deps import channel_service
from kronicle.services.channel_service import ChannelService
from kronicle.types.iso_datetime import IsoDateTime

health_check = APIRouter(tags=["Check health"])


@health_check.get("/live", include_in_schema=True)
def liveness():
    return {"status": "alive"}


@health_check.get("/ready", include_in_schema=True)
async def readiness(
    controller: ChannelService = Depends(channel_service),  # noqa: B008
):
    try:
        # Minimal DB probe
        is_ready: bool = await controller.ping()  # type: ignore[attr-defined]
        return {"status": "ready"} if is_ready else {"status": "not_ready"}
    except Exception as e:
        return {"status": "not_ready", "error": str(e)}


@health_check.get("/version", include_in_schema=True)
def version():
    return {"version": __version__, "commit": __commit__, "date": IsoDateTime(__build_date__)}
