# kronicle/services/seed_service.py
from kronicle.db.rbac.models.rbac_role import RbacRole
from kronicle.db.rbac.rbac_db_session import RbacDbSession
from kronicle.repo.rbac.entities.rbac_role_repo import RbacRoleRepository
from kronicle.schemas.permissions.permission_sets import DEFAULT_ROLES
from kronicle.utils.dev_logs import log_i


def seed_default_roles(rbac_db: RbacDbSession) -> None:
    repo = RbacRoleRepository()
    with rbac_db.transaction() as db:
        for role_def in DEFAULT_ROLES:
            existing = repo.get_by_name(db, name=role_def["name"])
            if existing:
                log_i("seed", f"Role '{role_def['name']}' already exists, skipping")
                continue
            role = RbacRole(
                name=role_def["name"],
                description=role_def.get("description", ""),
                permissions=role_def["permissions"],
                restrictions=role_def.get("restrictions", []),
            )
            db.add(role)
            db.flush()
            log_i("seed", f"Created default role '{role_def['name']}'")
