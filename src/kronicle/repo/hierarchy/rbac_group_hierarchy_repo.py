# kronicle/repo/hierarchy/rbac_group_hierarchy_repo.py


from kronicle.db.rbac.links.group_hierarchy import RbacGroupHierarchy
from kronicle.repo.kronicle_link_repo import KronicleLinkRepository


class RbacGroupHierarchyRepository(KronicleLinkRepository[RbacGroupHierarchy]):
    model = RbacGroupHierarchy
