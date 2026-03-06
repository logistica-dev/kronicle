"""
Create the Kronicle admin user in the main DB.

- Uses the b64url-encoded credentials from the config
- Idempotent: does nothing if user already exists
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from kronicle.db.rbac.models import RbacUser
from scripts.utils.logger import log_d  # type: ignore
from scripts.utils.read_conf import KronicleConf  # type: ignore

here = "init.03_create_superuser"


def main():
    conf: KronicleConf = KronicleConf.read_conf()
    su_name, su_pwd = conf.app_su.creds
    su_email = conf.app_su.email
    rbac_db_url = conf.db.dsn(creds=conf.rbac_creds)

    table_name = f"{RbacUser.namespace()}.{RbacUser.tablename()}"

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
