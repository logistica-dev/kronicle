# kronicle/db/core/models/core_base.py

from kronicle.db.base.kronicle_entity import KronicleEntity

_CORE_NAMESPACE = "core"


# ----------------------------------------------------------------------
# Base class for Resources tables
# ----------------------------------------------------------------------
class CoreEntity(KronicleEntity):
    """
    Base class for all resources Core tables.
    Provides a table structure validation method.
    """

    __abstract__ = True  # Do not create a table for this class itself
    __table_args__ = {"schema": _CORE_NAMESPACE, "extend_existing": True}

    @classmethod
    def namespace(cls):
        return _CORE_NAMESPACE

    @classmethod
    def table_args(cls):
        return {"schema": cls.namespace()}


if __name__ == "__main__":  # pragma: no cover
    print(CoreEntity.namespace())  # prints core => OK
    try:
        CoreEntity.namespace = "testsing"  # type: ignore
    except AttributeError:
        print("Caught AttributeError: OK")
    print(CoreEntity.namespace())  # prints core => OK
    print(CoreEntity.table_args())  # prints {'schema': 'core'} => OK
    print(CoreEntity.table())  # prints {'schema': 'core'} => OK
