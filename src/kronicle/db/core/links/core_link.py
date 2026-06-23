# kronicle/db/core/links/core_link.py

from kronicle.db.base.kronicle_link import KronicleLink
from kronicle.db.core.models.core_entity import CoreEntity


class CoreLink(KronicleLink):

    __abstract__ = True  # Do not create a table for this class itself

    PARENT_ID = "parent_id"
    CHILD_ID = "child_id"

    @classmethod
    def namespace(cls) -> str:
        return CoreEntity.namespace()
