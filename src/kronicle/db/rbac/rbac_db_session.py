# kronicle/db/rbac/rbac_db_session.py
from contextlib import contextmanager
from typing import Any, Callable, Generator

from sqlalchemy import create_engine, literal, select
from sqlalchemy.orm import Session, sessionmaker

from kronicle.db.rbac.models import ALL_RBAC_TABLES
from kronicle.utils.dev_logs import log_e

mod = "rbac_db"


class RbacDbSession:
    """
    Synchronous session manager for RBAC tables
    Provides session/connection context
    Thread-safe factory for sessions
    """

    def __init__(self, db_url: str, echo: bool = False):
        # Create a standard synchronous engine
        self._engine = create_engine(db_url, echo=echo, future=True)
        # Create a thread-safe session factory
        self._session_factory = sessionmaker(
            bind=self._engine,
            expire_on_commit=False,
            class_=Session,
        )

    # ----------------------------------------------------------------------------------------------
    # Session (fine for read only)
    # ----------------------------------------------------------------------------------------------
    @contextmanager
    def get_db(self) -> Generator[Session, None, None]:
        """
        Provide a synchronous session as a context manager.
        For read-only use: changes won’t persist unless session.commit() is explicitly called.

        Usage:
            with rbac_sessions.get_db() as db:
                user = db.query(RbacUser).filter_by(email="...").first()
        """
        session = self._session_factory()
        try:
            yield session
        finally:
            session.close()

    # ----------------------------------------------------------------------------------------------
    # Transaction (for write operations that need rollback)
    # ----------------------------------------------------------------------------------------------
    @contextmanager
    def transaction(self):
        """
        Yield a session inside a transaction context.
        This is what should be use when writing the DB.
        """
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ----------------------------------------------------------------------------------------------
    # Utility: execute with error interception
    # ----------------------------------------------------------------------------------------------
    def execute(self, func: Callable[[Session], Any], *, catch_errors: bool | None = True):
        """
        Execute a callable with a Session from transaction(), optionally intercepting errors.

        Example:
            result = rbac_sessions.execute(lambda db: db.query(RbacUser).first())
        """
        try:
            with self.get_db() as session:
                return func(session)
        except Exception as e:
            if catch_errors:
                log_e(f"{mod}.execute", f"Error intercepted: {e}")
                return None
            raise

    # ----------------------------------------------------------------------------------------------
    # Health check
    # ----------------------------------------------------------------------------------------------
    def ping(self) -> bool:
        """
        Check if the RBAC database is reachable.
        Returns True if a simple query succeeds, False otherwise.
        """
        try:
            with self.transaction() as session:
                session.execute(select(literal(1)))
            return True
        except Exception as e:
            log_e(f"{mod}.ping", f"Ping failed: {e}")
            return False

    # ----------------------------------------------------------------------------------------------
    # Startup table validation
    # ----------------------------------------------------------------------------------------------
    def validate_tables(self):
        """
        Validate that all declared RBAC tables exist and match the model structure.
        Raises RuntimeError if a table is missing or mismatched.
        """
        errors = []

        with self._engine.connect() as conn:
            for model in ALL_RBAC_TABLES:
                try:
                    model.validate_table(conn)
                except RuntimeError as e:
                    errors.append(str(e))

            if errors:
                raise RuntimeError("RBAC table validation failed:\n" + "\n".join(errors))

    # ----------------------------------------------------------------------------------------------
    # Shutdown
    # ----------------------------------------------------------------------------------------------
    def close(self):
        """
        Dispose the engine and close all connections in the pool.
        Call this on application shutdown.
        """
        if self._engine:
            self._engine.dispose()
