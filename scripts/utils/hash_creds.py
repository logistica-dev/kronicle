# scripts/hash_creds.py
"""
Generate a b64url-encoded Kronicle admin credential for the config file.
Usage:
    python scripts/hash_creds.py <username> <password>
Output:
    Prints b64url(<username>:<hashed_password>) string
"""

import sys

from kronicle.auth.pwd.pwd_manager import PasswordManager
from kronicle.auth.pwd.pwd_policy import PasswordPolicy
from kronicle.utils.str_utils import encode_b64url


def main():

    if len(sys.argv) not in [3, 4]:
        print("Usage: python3 scripts/utils/hash_creds.py <username> <email> <password>")
        sys.exit(1)

    username = sys.argv[1]

    if len(sys.argv) == 4:
        print("Hashing password and encoding su_name su_email SU_passw0rd into su_name:su_email:encrypted_SU_passw0rd")
        email = sys.argv[2]
        password = sys.argv[3]

        # Initialize a PasswordManager instance (defaults)
        PasswordManager.initialize(policy=PasswordPolicy(), time_cost=3, memory_cost=65536, parallelism=4)

        pm = PasswordManager()

        # Hash the password according to app's policy (Argon2id)
        hashed_pwd = pm.hash_password(password)

        # Combine <username>:<hashed_password> and encode as b64url
        combined = f"{username}:{email}:{hashed_pwd}"
    else:
        print("Encoding username clear_pasSw0rd into username:clear_pasSw0rd")
        password = sys.argv[2]
        combined = f"{username}:{password}"

    b64url_cred = encode_b64url(combined)

    print(f"b64url credential for config:\n{b64url_cred}")


if __name__ == "__main__":  # pragma: no cover
    main()
