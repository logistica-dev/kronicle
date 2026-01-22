"""
Create the Kronicle admin user in the main DB.

- Uses the b64url-encoded credentials from the config
- Idempotent: does nothing if user already exists
"""

from os import environ

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from kronicle.db.rbac.models import RbacUser
from kronicle.utils.dev_logs import log_d
from kronicle.utils.str_utils import decode_b64url

here = "init.03_create_superuser"


def get_env_var(var_name: str, errors: list) -> str | None:
    try:
        var_val = environ[var_name]
        if not var_val:
            errors.append(var_name)
        return var_val
    except Exception:
        errors.append(var_name)
    return


def get_conf():
    errors = []
    rbac_db_url = get_env_var("KRONICLE_RBAC_URL", errors)
    su_name = get_env_var("KRONICLE_SU_NAME", errors)
    su_email = get_env_var("KRONICLE_SU_EMAIL", errors)
    su_pwd = get_env_var("KRONICLE_SU_PWD", errors)  # encrypted then b64 encoded!
    if su_pwd:
        su_pwd = decode_b64url(su_pwd)
    if errors:
        raise RuntimeError(f"Env variables missing: {', '.join(errors)}")
    return rbac_db_url, su_name, su_email, su_pwd


def main():
    rbac_db_url, su_name, su_email, su_pwd = get_conf()
    table_name = f"{RbacUser.namespace()}.{RbacUser.tablename()}"

    assert rbac_db_url
    engine = create_engine(rbac_db_url, future=True)
    log_d(here, "Connected to Kronicle DB")

    # Use a session for ORM inserts
    with Session(engine) as session:
        # Check if admin user already exists
        existing_user = session.query(RbacUser).filter_by(name=su_name).first()
        if existing_user:
            log_d(here, f"Admin user '{su_name}' already exists")
            if not existing_user.is_superuser:
                existing_user.is_superuser = True
                session.commit()
                log_d(here, f"Updated '{su_name}' to be a superuser")
            else:
                log_d(here, f"Admin user '{su_name}' is already a superuser")

        else:
            admin_user = RbacUser(
                name=su_name,
                password_hash=su_pwd,
                email=su_email,
                is_active=True,
                is_superuser=True,
            )
            session.add(admin_user)
            session.commit()
            log_d(here, f"Created admin user '{su_name}' in {table_name}")


if __name__ == "__main__":
    main()
