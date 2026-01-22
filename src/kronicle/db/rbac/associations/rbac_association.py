# kronicle/db/base/kronicle_association.py

from kronicle.db.base.kronicle_association import KronicleAssociation
from kronicle.db.rbac.models.rbac_entity import RbacEntity


class RbacAssociation(KronicleAssociation):

    __abstract__ = True  # Do not create a table for this class itself

    @classmethod
    def namespace(cls):
        return RbacEntity.namespace()
