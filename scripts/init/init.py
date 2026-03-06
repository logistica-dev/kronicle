# scripts/init/init.py
"""
Minimal bootstrap for CI / first-time DB setup.

Runs all init scripts in order. Safe to re-run.
"""

from pathlib import Path
from subprocess import CalledProcessError, run
from sys import executable, exit, stderr

mod = "init.00_bootstrap"


def run_script(script_path: Path):
    """Run a single Python script via subprocess and print output."""
    print(f"[{mod}] Running {script_path.name} ...")
    try:
        result = run(
            [executable, str(script_path)],
            capture_output=True,
            text=True,
            check=True,
        )
        # Print stdout and stderr from the script
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=stderr)
    except CalledProcessError as e:
        print(f"[{mod}] ERROR: {script_path.name} failed with return code {e.returncode}", file=stderr)
        if e.stdout:
            print(e.stdout)
        if e.stderr:
            print(e.stderr, file=stderr)
        exit(e.returncode)
    print(f"[{mod}] Finished {script_path.name}\n")


def main():
    base_path = Path(__file__).parent
    scripts = [
        "01_bootstrap_db.py",  # create DB + Timescale extension
        "02_create_tables.py",  # schemas + tables
        "03_create_app_su.py",  # initial superuser
    ]

    for script_name in scripts:
        script_path = base_path / script_name
        if script_path.exists():
            run_script(script_path)
        else:
            print(f"[{mod}] WARNING: {script_name} not found, skipping.")

    print(f"[{mod}] Bootstrap completed successfully!")


if __name__ == "__main__":
    main()
