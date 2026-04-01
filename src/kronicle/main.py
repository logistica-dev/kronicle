# kronicle/main.py
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from json import dump
from pathlib import Path
from traceback import extract_tb

from fastapi import FastAPI, HTTPException, Request
from fastapi import HTTPException as FastApiHttpException
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import ValidationError
from sqlalchemy.orm import configure_mappers
from starlette.exceptions import HTTPException as StarletteHttpException

from kronicle.api.auth_routes import auth_router
from kronicle.api.health_check import health_check
from kronicle.api.rbac_routes import rbac_router
from kronicle.api.read_routes import reader_router
from kronicle.api.setup_routes import setup_router
from kronicle.api.write_routes import writer_router
from kronicle.auth.auth_middleware import AuthenticationMiddleware
from kronicle.auth.auth_service import AuthService
from kronicle.auth.jwt_service import JWTService
from kronicle.auth.pwd.pwd_manager import PasswordManager
from kronicle.auth.pwd.pwd_policy import PasswordPolicy
from kronicle.db.data.channel_db_session import ChannelDbSession
from kronicle.db.data.channel_repository import ChannelRepository
from kronicle.db.rbac.rbac_db_session import RbacDbSession
from kronicle.deps.settings import Settings
from kronicle.errors.error_types import KronicleAppError, KronicleHTTPErrorPayload
from kronicle.errors.exception_handlers import (
    app_error_adapter,
    fastapi_exception_adapter,
    generic_exception_handler,
    pydantic_exception_adapter,
)
from kronicle.logging.log_bus.mid_sanitize import RequestSanitizerMiddleware
from kronicle.services.channel_service import ChannelService
from kronicle.services.rbac_service import RbacService
from kronicle.utils.dev_logs import log_block, log_d, log_e, log_w, request_logger, setup_logging

mod = "main"


class KronicleApp:
    def __init__(self):
        print("-------------------------------------------------------------------------------------------------------")
        here = "init"
        # Setup logging
        setup_logging()
        log_d(here, "Logger ready")

        # Retrieving the configuration settings
        with log_block(here, "Configuration"):
            self.conf = Settings()

        # Initialize application logging
        log_d(here, self.conf.app.name, f"v{self.conf.app.version}", self.conf.app.description)
        log_d(here, f"Launching on {self.conf.server.host}:{self.conf.server.port}")

        # Lifespan takes care of the RBAC DB service right bellow
        self.rbac_service: RbacService

        # Initialize authentication
        with log_block(here, "PasswordManager"):
            self.policy = PasswordPolicy(
                min_length=self.conf.auth.pwd_min_length,
                require_uppercase=self.conf.auth.pwd_require_uppercase,
                require_lowercase=self.conf.auth.pwd_require_lowercase,
                require_digits=self.conf.auth.pwd_require_digits,
                require_special=self.conf.auth.pwd_require_special,
                special_chars=self.conf.auth.pwd_special_chars,
            )
            PasswordManager.initialize(policy=self.policy, time_cost=3, memory_cost=65536, parallelism=4)

        with log_block(here, "JWTService"):
            jwt_service = JWTService(self.conf.jwt)
            self.jwt_service = jwt_service

        # Create and configure the FastAPI application
        with log_block(here, "FastAPI and lifespan"):
            self.app: FastAPI = FastAPI(
                lifespan=self.lifespan,
                title=self.conf.app.name,
                debug=False,
                version=self.conf.app.version,
                summary=self.conf.app.description,
                openapi_url=self.conf.app.openapi_url if not self.conf.is_prod_env else None,
                docs_url="/docs" if not self.conf.is_prod_env else None,
                redoc_url="/redoc" if not self.conf.is_prod_env else None,
                redirect_slashes=False,
            )

        # Initialize middleware
        with log_block(here, "Middleware"):
            self.init_middleware()

        # Initialize exception handlers
        with log_block(here, "Exception handler"):
            self.init_exception_handlers()

        # Initialize routes
        with log_block(here, "Routes"):
            self.init_routes()

        # Generating OpenAPI doc
        if self.conf.is_dev_env:
            with log_block(here, "Doc generation"):
                self.generate_doc()

    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncIterator[None]:
        """Application lifecycle management."""
        here = "app.launch"
        # log_d(here, "Channel DB:", f"{self.conf.db._host}:{self.conf.db._port}/{self.conf.db._name}")
        log_d(here, "Connection url:", self.conf.db.masked_connection_url)
        with log_block(here, "Channel DB"):
            channel_db = ChannelDbSession(db_url=self.conf.db.channel_connection_url)
            await channel_db.init_async()
            self.channel_db = channel_db
        with log_block(here, "Channel deps"):
            channel_repository = ChannelRepository(channel_db)
            app.state.channel_service = ChannelService(channel_repository)

        with log_block(here, "RBAC session manager"):
            self.rbac_db = RbacDbSession(db_url=self.conf.db.rbac_connection_url, echo=False)
        with log_block(here, "RBAC mappers"):
            configure_mappers()
        with log_block(here, "RBAC tables validation"):
            self.rbac_db.validate_tables()  # Add all RBAC models here

        with log_block(here, "RBAC service"):
            rbac_service = RbacService(self.rbac_db)
            app.state.rbac_service = rbac_service

        with log_block(here, "AuthService"):
            app.state.auth_service = AuthService(self.jwt_service, rbac_service)

        # ----- Start background consumer tasks
        # setup_task = asyncio.create_task(consume_setup_logs())
        # data_task = asyncio.create_task(consume_data_logs())
        # api_task = asyncio.create_task(consume_api_logs())
        # log_d("Log consumers started")

        log_d(here, f"Swagger docs available at: http://{self.conf.server.host}:{self.conf.server.port}/docs")
        log_d(here, "Kronicle server ready")
        print("------------------------------------------------------------------------------------------[ Init OK ]--")

        yield

        # ----- Cleanup on shutdown
        with log_block(here, "Channel DB shutdown"):
            await self.channel_db.close()
        with log_block(here, "RBAC DB shutdown"):
            self.rbac_db.close()

        # Cancel background tasks if they were started
        # for task in [setup_task, data_task, api_task]:
        #     task.cancel()
        #     try:
        #         await task
        #     except asyncio.CancelledError:
        #         print(f"Task {task.get_name()} cancelled.")

    def init_routes(self):
        """Initialize all API routes."""
        api_version = self.conf.api_version

        # Health checks
        self.app.include_router(health_check, prefix="/health")
        self.app.include_router(auth_router, prefix=f"/auth/{api_version}")

        # Add read/write/setup routes
        self.app.include_router(reader_router, prefix=f"/api/{api_version}")
        self.app.include_router(writer_router, prefix=f"/data/{api_version}")
        self.app.include_router(setup_router, prefix=f"/setup/{api_version}")

        self.app.include_router(rbac_router, prefix=f"/rbac/{api_version}")

        # Add root and favicon routes
        @self.app.get("/", include_in_schema=False)
        def read_root():
            return {"app": self.conf.app.name}

        @self.app.get("/favicon.ico", include_in_schema=False)
        def favicon():
            favicon_path = Path(__file__).resolve().parents[2] / "static" / "favicon.ico"
            return FileResponse(favicon_path)

    def init_middleware(self):
        """Initialize application middleware."""

        # Add authentication middleware
        self.app.add_middleware(
            RequestSanitizerMiddleware,
            are_docs_public=not self.conf.is_prod_env,
        )

        # Add authentication middleware
        self.app.add_middleware(
            AuthenticationMiddleware,
            jwt_service=self.jwt_service,
            are_docs_public=not self.conf.is_prod_env,
        )

        # Add our own log pipelining middleware
        # app.add_middleware(LogPublisherMiddleware)

        # Add CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Add request logging middleware
        @self.app.middleware("http")
        async def log_requests(request: Request, call_next):
            request_logger.info(f"{request.method} {request.url.path}")
            return await call_next(request)

        # Add trailing slash stripping middleware
        @self.app.middleware("http")
        async def strip_trailing_slash(request: Request, call_next):
            if request.url.path != "/" and request.url.path.endswith("/"):
                request.scope["path"] = request.url.path.rstrip("/")
            return await call_next(request)

        @self.app.middleware("http")
        async def catch_all_exceptions(request: Request, call_next):
            here = "catch_all_exc"
            try:
                return await call_next(request)
            except (HTTPException, KronicleAppError, ValidationError) as exc:
                log_w(here, exc)
                raise exc
            except Exception as exc:
                log_e(here, type(exc).__name__)
                # Filter out .venv lines

                tb = extract_tb(exc.__traceback__)
                # log_w(here, "trace:", tb)
                filtered = [f for f in tb if "src/kronicle" in f.filename]

                # Make filenames relative for shorter output
                rel_root = Path(__file__).resolve().parents[2]  # adjust to project root
                for f in filtered:
                    rel_file = Path(f.filename).relative_to(rel_root)
                    log_e(here, f"{rel_file}:{f.lineno} ->", f.name)

                # Return standard JSON error
                return KronicleHTTPErrorPayload.from_exception(
                    request=request,
                    exc=exc,
                    status=500,
                    error="InternalServerError",
                    message="An unexpected error occurred.",
                ).to_error_json()

    def init_exception_handlers(self):
        """Initialize exception handlers."""
        self.app.add_exception_handler(KronicleAppError, app_error_adapter)
        self.app.add_exception_handler(StarletteHttpException, fastapi_exception_adapter)
        self.app.add_exception_handler(FastApiHttpException, fastapi_exception_adapter)
        self.app.add_exception_handler(RequestValidationError, pydantic_exception_adapter)
        self.app.add_exception_handler(ValidationError, pydantic_exception_adapter)
        self.app.add_exception_handler(Exception, generic_exception_handler)

    def generate_doc(self):
        docs_path = Path(__file__).resolve().parents[2] / "docs" / "openapi.json"
        with open(docs_path, "w") as f:
            dump(self.app.openapi(), f, indent=2)

        print("OpenAPI spec written to openapi.json")


# Create the application and expose an app to uvicorn
app = KronicleApp().app
