# kronicle/db/rbac/links/group_hierarchy.py

from sqlalchemy import UniqueConstraint

from kronicle.db.base.kronicle_hierarchy import KronicleHierarchy
from kronicle.db.rbac.links.rbac_link import RbacLink
from kronicle.db.rbac.models.rbac_entity import RbacEntity
from kronicle.db.rbac.models.rbac_group import RbacGroup


class RbacGroupHierarchy(KronicleHierarchy[RbacGroup]):
    """
    RBAC group hierarchy definition.

    - Can be a DAG or tree depending on configuration
    - Used for permission inheritance
    """

    node_model = RbacGroup

    UQ_CONSTRAINT = "uq_group_parent"

    __tablename__ = "group_hierarchy"
    __table_args__ = (
        UniqueConstraint(
            KronicleHierarchy.CHILD_ID,
            KronicleHierarchy.PARENT_ID,
            name=UQ_CONSTRAINT,
        ),  # Tuple of constraints first
        {"schema": RbacLink.namespace(), "extend_existing": True},  # Options dictionary last
    )

    @classmethod
    def namespace(cls) -> str:
        return RbacEntity.namespace()
