# kronicle/repo/hierarchy/rbac_group_hierarchy_repo.py


from kronicle.db.rbac.links.group_hierarchy import RbacGroupHierarchy
from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.repo.hierarchy.hierarchy_repo import KronicleHierarchyRepo


class RbacGroupHierarchyRepository(KronicleHierarchyRepo[RbacGroupHierarchy, RbacGroup]):
    model = RbacGroupHierarchy
    node_model = RbacGroup
