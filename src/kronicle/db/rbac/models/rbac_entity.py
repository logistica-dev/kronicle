# kronicle/db/rbac/models/rbac_entity.py


from kronicle.db.base.kronicle_entity import KronicleEntity

_RBAC_NAMESPACE = "rbac"


# ----------------------------------------------------------------------
# Base class for RBAC tables
# ----------------------------------------------------------------------
class RbacEntity(KronicleEntity):
    """
    Base class for all RBAC tables.
    Provides a table structure validation method.
    """

    __abstract__ = True  # Do not create a table for this class itself
    __table_args__ = {"schema": _RBAC_NAMESPACE, "extend_existing": True}

    @classmethod
    def namespace(cls):
        return _RBAC_NAMESPACE

    @classmethod
    def table_args(cls):
        return {"schema": cls.namespace()}


if __name__ == "__main__":  # pragma: no cover
    print(RbacEntity.namespace())  # prints rbac => OK
    try:
        RbacEntity.namespace = "testing"  # type: ignore
    except AttributeError:
        print("Caught AttributeError: OK")
    print(RbacEntity.namespace())  # prints rbac => OK
    print(RbacEntity.table_args())  # prints rbac => OK
