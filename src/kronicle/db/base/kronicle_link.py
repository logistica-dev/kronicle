# kronicle/db/base/kronicle_link.py
from __future__ import annotations

from kronicle.db.base.kronicle_base import KronicleBase


class KronicleLink(KronicleBase):
    """
    Association/connection between KronicleEntities

    The Primary Key in such associations is always a (unique) couple representing an association.
    Thus no id (UUID) here.
    """

    __abstract__ = True  # Do not create a table for this class itself

    UQ_CONSTRAINT: str

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Skip abstract classes
        if getattr(cls, "__abstract__", False):
            return

        if not getattr(cls, "UQ_CONSTRAINT", None):
            raise TypeError(f"{cls.__name__} must define UQ_CONSTRAINT")

        table = getattr(cls, "__table__", None)
        if table is not None and not table.primary_key.columns:
            raise TypeError(f"{cls.__name__} has no primary key columns")

    @classmethod
    def uq_constraint(cls) -> str:
        if not cls.UQ_CONSTRAINT:
            raise NotImplementedError("KronicleLink classes should define UQ_CONSTRAINT")
        return cls.UQ_CONSTRAINT
