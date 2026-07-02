# kronicle/repo/rbac/entities/rbac_policy_repo.py


from kronicle.db.rbac.links.rbac_policy import RbacPolicy
from kronicle.repo.kronicle_repo import KronicleRepository


class RbacRoleRepository(KronicleRepository[RbacPolicy]):

    model = RbacPolicy
