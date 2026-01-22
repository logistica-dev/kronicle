# kronicle/api/health_check.py

from fastapi import APIRouter, Depends

from kronicle.deps.channel_deps import channel_service
from kronicle.services.channel_service import ChannelService

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
