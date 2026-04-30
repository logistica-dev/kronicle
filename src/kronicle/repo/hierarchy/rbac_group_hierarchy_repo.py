# kronicle/db/rbac/repo/links/rbac_group_roles_repo.py


from kronicle.db.rbac.links.group_hierarchy import RbacGroupHierarchy
from kronicle.repo.kronicle_link_repo import KronicleLinkRepository


class RbacGroupHierarchyRepository(KronicleLinkRepository[RbacGroupHierarchy]):
    model = RbacGroupHierarchy
