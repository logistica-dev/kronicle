# kronicle/repo/rbac/entities/rbac_role_repo.py


from kronicle.db.rbac.models.rbac_role import RbacRole
from kronicle.repo.kronicle_repo import KronicleRepository


class RbacRoleRepository(KronicleRepository[RbacRole]):

    model = RbacRole
