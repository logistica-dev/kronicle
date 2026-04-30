# kronicle/db/rbac/repo/rbac_group_repo.py


from kronicle.db.rbac.models.rbac_role import RbacRole
from kronicle.repo.kronicle_repo import KronicleRepository


class RbacRoleRepository(KronicleRepository[RbacRole]):
    pass
