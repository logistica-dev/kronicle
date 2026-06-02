# kronicle/db/rbac/links/group_roles.py
from uuid import UUID, uuid4

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kronicle.db.rbac.links.rbac_link import RbacLink
from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_role import RbacRole


class RbacGroupRoles(RbacLink):

    UQ_CONSTRAINT = "uq_group_roles"

    __tablename__ = "group_roles"
    __table_args__ = (
        UniqueConstraint(RbacLink.GROUP_ID, RbacLink.ROLE_ID, name=UQ_CONSTRAINT),  # Tuple of constraints first
        {"schema": RbacLink.namespace(), "extend_existing": True},  # Options dictionary last
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), default=uuid4)

    group_id: Mapped[UUID] = mapped_column(ForeignKey(RbacGroup.id), primary_key=True)
    role_id: Mapped[UUID] = mapped_column(ForeignKey(RbacRole.id), primary_key=True)

    # Optional ORM helpers
    group: Mapped[RbacGroup] = relationship(RbacGroup, backref=__tablename__)
    role: Mapped[RbacRole] = relationship(RbacRole, backref=__tablename__)
