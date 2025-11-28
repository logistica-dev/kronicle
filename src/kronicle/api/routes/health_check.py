# kronicle/routes/health_check.py

from fastapi import APIRouter, Depends

from kronicle.controller.sensor_controller import SensorController
from kronicle.core.deps import get_sensor_controller

health_check = APIRouter()


@health_check.get("/live", include_in_schema=True)
def liveness():
    return {"status": "alive"}


@health_check.get("/ready", include_in_schema=True)
async def readiness(
    controller: SensorController = Depends(get_sensor_controller),  # noqa: B008
):
    try:
        # Minimal DB probe
        is_ready: bool = await controller.ping()  # type: ignore[attr-defined]
        return {"status": "ready"} if is_ready else {"status": "not_ready"}
    except Exception as e:
        return {"status": "not_ready", "error": str(e)}
