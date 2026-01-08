from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from kronicle.log_bus.event_bus import (
    answers_queue,
    api_queue,
    data_queue,
    setup_queue,
)
from kronicle.types.iso_datetime import IsoDateTime


class LogPublisherMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        req_ip = "unkown" if request.client is None else request.client.host

        in_log_event = {
            "date_received": IsoDateTime.now_local(),
            "path": request.url.path,
            "method": request.method,
            "ip": req_ip,
        }

        if request.url.path.startswith("/setup"):
            in_log_event["url_path"] = "/setup"
            await setup_queue.put(in_log_event)
        elif request.url.path.startswith("/data"):
            in_log_event["url_path"] = "/data"
            await data_queue.put(in_log_event)
        elif request.url.path.startswith("/api"):
            in_log_event["url_path"] = "/api"
            await api_queue.put(in_log_event)

        response = await call_next(request)

        out_log_event = {
            "date_answered": IsoDateTime.now_local(),
            "path": request.url.path,
            "method": request.method,
            "status_code": response.status_code,
            "ip": req_ip,
        }
        await answers_queue.put(out_log_event)

        return response
