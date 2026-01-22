"""
Bootstrap script: first-time DB setup.

Executes, in order:
1. TimescaleDB extension
2. Main database user + DB creation
3. RBAC tables and schema

Safe to run multiple times (scripts themselves are idempotent).
"""

import subprocess
from pathlib import Path

mod = "init.00_bootstrap"


def run_script(script_path: Path):
    print(f"[{mod}] Running {script_path.name} ...")
    subprocess.run(["python", str(script_path)], check=True)
    print(f"[{mod}] Finished {script_path.name}\n")


def main():
    base_path = Path(__file__).parent
    scripts = [
        "01_bootstrap_db.py",
        "02_create_tables.py",
        "03_create_superuser.py",
    ]

    for script_name in scripts:
        script_path = base_path / script_name
        run_script(script_path)

    print(f"[{mod}] Bootstrap completed successfully!")


if __name__ == "__main__":  # pragma: no cover
    main()
