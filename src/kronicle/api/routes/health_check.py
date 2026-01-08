# kronicle/routes/health_check.py

from fastapi import APIRouter, Depends

from kronicle.controller.operation_gate import OperationGate
from kronicle.core.deps import get_operation_gate

health_check = APIRouter()


@health_check.get("/live", include_in_schema=True)
def liveness():
    return {"status": "alive"}


@health_check.get("/ready", include_in_schema=True)
async def readiness(
    controller: OperationGate = Depends(get_operation_gate),  # noqa: B008
):
    try:
        # Minimal DB probe
        is_ready: bool = await controller.ping()  # type: ignore[attr-defined]
        return {"status": "ready"} if is_ready else {"status": "not_ready"}
    except Exception as e:
        return {"status": "not_ready", "error": str(e)}
