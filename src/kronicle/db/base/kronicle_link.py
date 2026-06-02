# kronicle/db/base/kronicle_link.py

from kronicle.db.base.kronicle_table import KronicleTable


class KronicleLink(KronicleTable):
    """
    Association/connection between KronicleEntities
    """

    __abstract__ = True  # Do not create a table for this class itself

    PARENT_ID = "parent_id"
    CHILD_ID = "child_id"

    PARENT_LINKS = "parent_links"
    children = "children"

    UQ_CONSTRAINT: str

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Skip abstract classes
        if getattr(cls, "__abstract__", False):
            return

        if not getattr(cls, "UQ_CONSTRAINT", None):
            raise TypeError(f"{cls.__name__} must define UQ_CONSTRAINT")

        table = getattr(cls, "__table__", None)
        if table is not None:
            constraint_names = {c.name for c in table.constraints}
            if cls.UQ_CONSTRAINT not in constraint_names:
                raise TypeError(
                    f"{cls.__name__}: UQ_CONSTRAINT '{cls.UQ_CONSTRAINT}' " f"not found in table constraints"
                )

    @classmethod
    def uq_constraint(cls) -> str:
        if not cls.UQ_CONSTRAINT:
            raise NotImplementedError("KronicleLink classes should define UQ_CONSTRAINT")
        return cls.UQ_CONSTRAINT
