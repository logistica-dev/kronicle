# kronicle/repo/hierarchy/hierarchy_repo.py
from typing import Generic, Type, TypeVar

from sqlalchemy.orm import Session

from kronicle.db.base.kronicle_hierarchy import KronicleHierarchy
from kronicle.repo.kronicle_link_repo import KronicleLinkRepository, T

N = TypeVar("N", bound=KronicleHierarchy)


class KronicleHierarchyRepo(KronicleLinkRepository[T], Generic[T, N]):
    """
    model = LINK (ZoneHierarchy)
    node_model = ENTITY (CoreZone).
    """

    node_model: Type[N]

    def add_parent(self, db: Session, parent: N, child: N) -> None:
        self.ensure_link(db, {KronicleHierarchy.PARENT_ID: parent.id, KronicleHierarchy.CHILD_ID: child.id})

    def remove_parent(self, db: Session, parent: N, child: N) -> None:
        self.remove_link(db, {KronicleHierarchy.PARENT_ID: parent.id, KronicleHierarchy.CHILD_ID: child.id})

    def list_parents(self, db: Session, node: N) -> list[N]:
        """
        List direct parents of a node
        """
        links: list[T] = self.list_links(db, filters={KronicleHierarchy.CHILD_ID: node.id})
        return [ln.parent for ln in links]

    def list_children(self, db: Session, node: N) -> list[N]:
        """
        List direct children of a node
        """
        links = self.list_links(db, filters={KronicleHierarchy.PARENT_ID: node.id})
        return [ln.child for ln in links]
