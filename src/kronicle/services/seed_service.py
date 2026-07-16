# kronicle/services/seed_service.py
from kronicle.db.rbac.models.rbac_group import RbacGroup
from kronicle.db.rbac.models.rbac_role import RbacRole
from kronicle.db.rbac.rbac_db_session import RbacDbSession
from kronicle.deps.rbac_defaults import ANONYMOUS_NAME, DEFAULT_ROLES
from kronicle.repo.rbac.entities.rbac_group_repo import RbacGroupRepository
from kronicle.repo.rbac.entities.rbac_role_repo import RbacRoleRepository
from kronicle.repo.rbac.entities.rbac_subject_repo import RbacSubjectRepository
from kronicle.utils.dev_logs import log_i

mod = "seed"


def seed_default_roles(rbac_db: RbacDbSession) -> None:
    here = "roles"
    role_repo = RbacRoleRepository()
    with rbac_db.transaction() as db:
        for role_def in DEFAULT_ROLES:
            existing = role_repo.get_by_name(db, name=role_def["name"])
            if existing:
                log_i(f"{mod}.{here}", f"Role '{role_def['name']}' already exists, skipping")
                continue
            role = RbacRole(
                name=role_def["name"],
                description=role_def.get("description", ""),
                permissions=role_def["permissions"],
                restrictions=role_def.get("restrictions", []),
                details={"seed": True},
            )
            role_repo.add(db, entity=role)
            log_i(f"{mod}.{here}", f"Created default role '{role_def['name']}'")


def seed_anonymous_group(rbac_db: RbacDbSession, allow_anonymous: bool = False) -> None:
    """Create the 'Anonymous' group if it doesn't exist."""
    here = "anonymous"
    group_repo = RbacGroupRepository()
    subject_repo = RbacSubjectRepository()
    with rbac_db.transaction() as db:
        existing = group_repo.get_by_name(db, name=ANONYMOUS_NAME)
        if allow_anonymous:
            if existing:
                log_i(f"{mod}.{here}", f"Group '{ANONYMOUS_NAME}' already exists, skipping")
                return
            log_i(f"{mod}.{here}", f"Creating group '{ANONYMOUS_NAME}'")
            group = RbacGroup(name=ANONYMOUS_NAME, details={"seed": True})
            group_repo.add(db, entity=group)
            subject_repo.ensure_from_group(db, group=group)
            return

        if existing:
            log_i(f"{mod}.{here}", f"Removing group '{ANONYMOUS_NAME}'")
            group_repo.delete(db, entity=existing)
            return
        log_i(f"{mod}.{here}", f"Group '{ANONYMOUS_NAME}' doesn't exist, skipping")
        return
