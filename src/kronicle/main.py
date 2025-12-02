# kronicle/main.py
from contextlib import asynccontextmanager
from json import dump
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from kronicle.api.exception_handlers import (
    app_error_handler,
    generic_exception_handler,
)
from kronicle.api.routes.health_check import health_check
from kronicle.api.routes.read_routes import reader_router
from kronicle.api.routes.setup_routes import setup_router
from kronicle.api.routes.write_routes import writer_router
from kronicle.core.deps import close_db, get_sensor_controller
from kronicle.core.ini_settings import conf
from kronicle.types.errors import AppError
from kronicle.utils.dev_logs import log_d, request_logger

mod = "main"

app_settings = conf.app


@asynccontextmanager
async def lifespan(app: FastAPI):
    here = "app.lifespan"
    # ----- Start background consumer tasks
    # setup_task = asyncio.create_task(consume_setup_logs())
    # data_task = asyncio.create_task(consume_data_logs())
    # api_task = asyncio.create_task(consume_api_logs())
    # log_d("Log consumers started")

    log_d(here, "Startup: initializing the DB controller...")
    await get_sensor_controller()
    log_d(here, "DB is ready.")
    log_d(here, f"Swagger docs available at: http://{app_settings.host}:{app_settings.port}/docs")
    log_d("------------------------—------------------------—------------------------—---[ Init OK ]--")

    yield

    # ----- Cleanup on shutdown
    log_d(here, "Shutdown: closing the DB connection/pool")
    await close_db()
    log_d(here, "DB connection shut down")

    # for task in [setup_task, data_task, api_task]:
    #     task.cancel()
    #     try:
    #         await task
    #     except asyncio.CancelledError:
    #         print(f"Task {task.get_name()} cancelled.")


log_d(mod, f"{app_settings.name} v{app_settings.version}", app_settings.description)


app = FastAPI(
    lifespan=lifespan,
    title=app_settings.name,
    debug=False,
    version=app_settings.version,
    summary=app_settings.description,
    openapi_url=app_settings.openapi_url,
    redirect_slashes=False,
)

app.add_exception_handler(AppError, app_error_handler)
app.add_exception_handler(Exception, generic_exception_handler)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    request_logger.info(f"{request.method} {request.url.path}")
    response = await call_next(request)
    return response


# Add authentication middleware
# app.add_middleware(RequestSanitizerMiddleware)

# Add authentication middleware
# app.add_middleware(AuthenticationMiddleware)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def strip_trailing_slash(request: Request, call_next):
    if request.url.path != "/" and request.url.path.endswith("/"):
        request.scope["path"] = request.url.path.rstrip("/")
    return await call_next(request)


# Add our own log pipelining middleware
# app.add_middleware(LogPublisherMiddleware)


# Root route
@app.get("/", include_in_schema=False)
def read_root():
    return {"app": app_settings.name}


# Root route
@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    favicon_path = Path(__file__).resolve().parents[2] / "static" / "favicon.ico"
    return FileResponse(favicon_path)


# Health checks
app.include_router(health_check, prefix="/health")

# Add read/write/setup routes
app.include_router(reader_router, prefix=f"/api/{conf.api_version}")
app.include_router(writer_router, prefix=f"/data/{conf.api_version}")
app.include_router(setup_router, prefix=f"/setup/{conf.api_version}")


with open("../docs/openapi.json", "w") as f:
    dump(app.openapi(), f, indent=2)

print("OpenAPI spec written to openapi.json")
