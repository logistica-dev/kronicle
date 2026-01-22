# kronicle/db/base/kronicle_association.py

from kronicle.db.base.kronicle_base import KronicleBase


class KronicleAssociation(KronicleBase):

    __abstract__ = True  # Do not create a table for this class itself
