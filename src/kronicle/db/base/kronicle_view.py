# kronicle/db/base/kronicle_view.py
from kronicle.db.base.kronicle_base import Base


class KronicleView(Base):
    """
    Mixin for virtual tables / views.
    Provides helper attributes / methods for view creation.
    """

    __abstract__ = True  # optional if you want it to never be instantiated directly

    is_view: bool = True  # marker

    @classmethod
    def namespace(cls):
        raise NotImplementedError("Method namespace() should be implemented at lower levels")

    @classmethod
    def tablename(cls):
        if hasattr(cls, "__tablename__"):
            return cls.__tablename__
        raise NotImplementedError("This is most likely a abstract class and doesn't map to a table")

    @classmethod
    def table(cls):
        return f"{cls.namespace()}.{cls.tablename()}"

    @classmethod
    def create_view_sql(cls) -> str:
        """Return the SQL to create the view. Must be overridden."""
        raise NotImplementedError
