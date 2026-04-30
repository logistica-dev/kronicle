# kronicle/db/base/kronicle_link.py

from kronicle.db.base.kronicle_base import KronicleBase


class KronicleLink(KronicleBase):
    """
    Association/connection between KronicleEntities
    """

    __abstract__ = True  # Do not create a table for this class itself

    PARENT_ID = "parent_id"
    CHILD_ID = "child_id"

    UQ_CONSTRAINT: str

    @classmethod
    def uq_constraint(cls) -> str:
        if not cls.UQ_CONSTRAINT:
            raise NotImplementedError("KronicleLink classes should define UQ_CONSTRAINT")
        return cls.UQ_CONSTRAINT
