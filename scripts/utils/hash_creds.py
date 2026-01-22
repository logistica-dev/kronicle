# scripts/hash_creds.py
"""
Generate a b64url-encoded Kronicle admin credential for the config file.
Usage:
    python scripts/hash_creds.py <username> <password>
Output:
    Prints b64url(<username>:<hashed_password>) string
"""

import base64
import sys

from kronicle.auth.pwd.pwd_manager import PasswordManager


def main():
    if len(sys.argv) != 3:
        print("Usage: python scripts/hash_creds.py <username> <password>")
        sys.exit(1)

    username = sys.argv[1]
    password = sys.argv[2]

    # Initialize a PasswordManager instance (defaults)
    pm = PasswordManager()

    # Hash the password according to app's policy (Argon2id)
    hashed_pwd = pm.hash_password(password)

    # Combine <username>:<hashed_password> and encode as b64url
    combined = f"{username}:{hashed_pwd}"
    b64url_cred = base64.urlsafe_b64encode(combined.encode("utf-8")).decode("utf-8")

    print(f"b64url credential for config:\n{b64url_cred}")


if __name__ == "__main__":  # pragma: no cover
    main()
