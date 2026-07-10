#!/usr/bin/env python3
"""Sync dependency files: requirements.txt and requirements-dev.txt.

Uses:
- AST scanning of src/ and tests/ to find actual imports
- importlib.metadata.packages_distributions() to map import names → package names
- uv pip list --outdated to get latest available versions
- deptry to cross-check unused/missing dependencies
"""

from __future__ import annotations

import ast
import json
import subprocess
import sys
from importlib.metadata import packages_distributions
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
TESTS_DIR = PROJECT_ROOT / "tests"
PROD_REQ = PROJECT_ROOT / "requirements.txt"
DEV_REQ = PROJECT_ROOT / "requirements-dev.txt"
RUN_REQ = PROJECT_ROOT / "requirements-run.txt"

KNOWN_DEV_PACKAGES = {
    "black",
    "commitizen",
    "deptry",
    "flake8",
    "kronicle-sdk",
    "nbstripout",
    "pip",
    "pip-chill",
    "pip-upgrade-outdated",
    "pre-commit",
    "pytest",
    "pytest-asyncio",
    "pytest-cov",
    "ruff",
    "setuptools",
    "wheel",
}

# Python package extras implied by certain from-imports
# e.g. `from sqlalchemy.dialects.postgresql import ...` → sqlalchemy[postgresql]
EXTRAS_MAP: dict[str, dict[str, str | None]] = {
    "sqlalchemy": {
        "postgresql": "postgresql",
        "mysql": "mysql",
        "mssql": "mssql",
        "oracle": "oracle",
    },
}


def get_stdlib_modules() -> set[str]:
    if hasattr(sys, "stdlib_module_names"):
        return set(sys.stdlib_module_names)
    return set()


def scan_imports(directory: Path) -> set[str]:
    imports: set[str] = set()
    if not directory.exists():
        return imports
    for path in sorted(directory.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.add(node.module.split(".")[0])
    return imports


def scan_extra_imports(directory: Path) -> dict[str, set[str]]:
    """Scan for from-imports that imply package extras.

    Returns {package_name: {extra, ...}}.
    """
    extras: dict[str, set[str]] = {}
    if not directory.exists():
        return extras
    for path in sorted(directory.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                parts = node.module.split(".")
                if len(parts) >= 3 and parts[1] == "dialects":
                    pkg = parts[0]
                    dialect = parts[2]
                    if pkg in EXTRAS_MAP and dialect in EXTRAS_MAP[pkg]:
                        extras.setdefault(pkg, set()).add(dialect)
    return extras


def get_import_to_package_map() -> dict[str, str]:
    """Return mapping of top-level import name → distribution (PyPI) name, lowercased."""
    mapping: dict[str, str] = {}
    for mod_name, dists in packages_distributions().items():
        for dist in dists:
            mapping[mod_name.lower()] = dist
    # Additional known overrides where the import name ≠ package name
    manual_overrides = {
        "pil": "Pillow",
        "yaml": "PyYAML",
        "cv2": "opencv-python",
        "sklearn": "scikit-learn",
        "dotenv": "python-dotenv",
        "nacl": "PyNaCl",
        "cryptography": "cryptography",
    }
    mapping.update(manual_overrides)
    return mapping


def read_req_file(path: Path) -> set[str]:
    """Read package names from an existing requirements file (ignoring versions/comments)."""
    if not path.exists():
        return set()
    pkgs: set[str] = set()
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name = line.split("=")[0].split(">")[0].split("<")[0].split("[")[0].strip()
        if name:
            pkgs.add(name)
    return pkgs


def run_uv_list() -> list[dict]:
    result = subprocess.run(
        ["uv", "pip", "list", "--format=json"],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def run_uv_outdated() -> dict[str, str]:
    result = subprocess.run(
        ["uv", "pip", "list", "--outdated", "--format=json"],
        capture_output=True,
        text=True,
        check=True,
    )
    return {pkg["name"]: pkg["latest_version"] for pkg in json.loads(result.stdout)}


def main() -> None:  # noqa: C901
    stdlib = get_stdlib_modules()

    print("=== Scanning imports in src/ and tests/ ===", flush=True)
    src_imports = scan_imports(SRC_DIR)
    test_imports = scan_imports(TESTS_DIR)

    internal_modules = {"kronicle", "__future__", *stdlib}
    for d in SRC_DIR.rglob("*"):
        if d.is_dir() and not d.name.startswith("_") and not d.name.startswith("."):
            internal_modules.add(d.name)

    src_imports -= internal_modules
    test_imports -= internal_modules

    print(f"  src/  imports: {len(src_imports)} unique top-level modules", flush=True)
    print(f"  tests/ imports: {len(test_imports)} unique top-level modules", flush=True)

    print("\n=== Building import → package mapping ===", flush=True)
    import_map = get_import_to_package_map()

    src_packages: dict[str, str] = {}
    for imp in sorted(src_imports):
        pkg = import_map.get(imp.lower())
        if pkg:
            src_packages[imp] = pkg
        else:
            # Try to find by checking if import name matches a package name
            print(f"  [warn] '{imp}' → no package mapping found", flush=True)

    test_only_packages: dict[str, str] = {}
    for imp in sorted(test_imports - src_imports):
        pkg = import_map.get(imp.lower())
        if pkg:
            test_only_packages[imp] = pkg

    print(f"  Mapped {len(src_packages)} src imports to packages", flush=True)
    print(f"  Mapped {len(test_only_packages)} test-only imports to packages", flush=True)

    print("\n=== Scanning for implied extras (e.g. sqlalchemy[postgresql]) ===", flush=True)
    src_extras = scan_extra_imports(SRC_DIR)
    test_extras = scan_extra_imports(TESTS_DIR)
    all_extras: dict[str, set[str]] = {}
    for pkg, dials in src_extras.items():
        all_extras.setdefault(pkg, set()).update(dials)
    for pkg, dials in test_extras.items():
        all_extras.setdefault(pkg, set()).update(dials)
    if all_extras:
        for pkg, dials in sorted(all_extras.items()):
            print(f"  {pkg}[{','.join(sorted(dials))}]", flush=True)
    else:
        print("  (none found)", flush=True)

    print("\n=== Getting installed packages & latest versions ===", flush=True)
    installed = run_uv_list()
    version_map: dict[str, str] = {p["name"]: p["version"] for p in installed}
    latest_map = run_uv_outdated()
    for name, ver in latest_map.items():
        if name in version_map:
            version_map[name] = ver  # use latest available

    # Identify the project package itself (not other editable installs)
    project_package_name: str | None = None
    for p in installed:
        loc = p.get("editable_project_location")
        if loc and Path(loc).resolve() == PROJECT_ROOT.resolve():
            project_package_name = p["name"]
            break

    print(f"\n=== Reading {RUN_REQ.name} (manual run deps) ===", flush=True)
    run_pkg_names = read_req_file(RUN_REQ)
    if run_pkg_names:
        print(f"  Found {len(run_pkg_names)} manually listed run deps", flush=True)
        for p in sorted(run_pkg_names):
            print(f"    {p}", flush=True)
    else:
        print(f"  (none — create {RUN_REQ.name} if you have CLI-only runtime deps)", flush=True)

    print("\n=== Classifying dependencies ===", flush=True)

    # Build set of package names that are actually direct deps (imported in code)
    prod_pkg_names: set[str] = set()
    for pkg in src_packages.values():
        # Normalize case: installed package names are typically lowercase
        for installed_name in version_map:
            if installed_name.lower() == pkg.lower():
                prod_pkg_names.add(installed_name)
                break
        else:
            prod_pkg_names.add(pkg)

    dev_pkg_names: set[str] = set()
    for pkg in test_only_packages.values():
        for installed_name in version_map:
            if installed_name.lower() == pkg.lower():
                dev_pkg_names.add(installed_name)
                break
        else:
            dev_pkg_names.add(pkg)

    # Known dev packages
    dev_pkg_names.update(KNOWN_DEV_PACKAGES)

    # Remove project package (e.g. 'kronicle') — not a dependency
    exclude = {project_package_name} if project_package_name else set()
    prod_pkg_names -= exclude
    dev_pkg_names -= exclude
    run_pkg_names -= exclude

    # Only keep packages that are actually installed
    prod_pkg_names &= set(version_map.keys())
    dev_pkg_names &= set(version_map.keys())
    run_pkg_names &= set(version_map.keys())

    # Remove prod packages from dev and run
    dev_pkg_names -= prod_pkg_names
    run_pkg_names -= prod_pkg_names
    run_pkg_names -= dev_pkg_names

    # Build the output dicts
    prod_packages = {p: version_map[p] for p in sorted(prod_pkg_names)}
    dev_packages = {p: version_map[p] for p in sorted(dev_pkg_names)}
    run_packages = {p: version_map[p] for p in sorted(run_pkg_names)}

    # Everything else installed is "other" (transitive deps or truly unused)
    other_packages = {
        p: version_map[p] for p in sorted(set(version_map) - prod_pkg_names - dev_pkg_names - run_pkg_names - exclude)
    }

    print(f"\n  Prod ({len(prod_packages)}):", flush=True)
    for p, v in prod_packages.items():
        print(f"    {p}=={v}", flush=True)
    print(f"  Dev ({len(dev_packages)}):", flush=True)
    for p, v in dev_packages.items():
        print(f"    {p}=={v}", flush=True)
    print(f"  Run (manual, from {RUN_REQ.name}) ({len(run_packages)}):", flush=True)
    for p, v in run_packages.items():
        print(f"    {p}=={v}", flush=True)
    print(f"  Other/transitive ({len(other_packages)}):", flush=True)
    for p, v in list(other_packages.items())[:10]:
        print(f"    {p}=={v}", flush=True)
    if len(other_packages) > 10:
        print(f"    ... and {len(other_packages) - 10} more", flush=True)

    print(f"\n=== Writing {PROD_REQ.name} ===", flush=True)
    with open(PROD_REQ, "w") as f:
        for pkg, ver in prod_packages.items():
            extras = all_extras.get(pkg)
            if extras:
                extra_str = f"[{','.join(sorted(extras))}]"
                f.write(f"{pkg}{extra_str}=={ver}\n")
            else:
                f.write(f"{pkg}=={ver}\n")
    print(f"  Wrote {len(prod_packages)} packages", flush=True)

    print(f"\n=== Writing {DEV_REQ.name} ===", flush=True)
    with open(DEV_REQ, "w") as f:
        for pkg, ver in dev_packages.items():
            f.write(f"{pkg}=={ver}\n")
    print(f"  Wrote {len(dev_packages)} packages", flush=True)

    print(f"\n=== Updating {RUN_REQ.name} ===", flush=True)
    if run_packages:
        with open(RUN_REQ, "w") as f:
            f.write("# requirements-run.txt — manually maintained CLI runtime deps\n")
            f.write("# (not detected by import scanner; add packages needed to run the app)\n")
            for pkg, ver in run_packages.items():
                f.write(f"{pkg}=={ver}\n")
        print(f"  Updated {len(run_packages)} packages", flush=True)
    else:
        print("  (nothing to write)", flush=True)

    print("\n=== Refreshing uv.lock ===", flush=True)
    subprocess.run(["uv", "lock"], capture_output=True, check=False)
    print("  Done", flush=True)

    print("\n=== Cross-check: deptry ===", flush=True)
    result = subprocess.run(
        ["uv", "tool", "run", "deptry", ".", "--json-output", "/tmp/deptry_check.json"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print("  deptry found issues (some may be false positives)", flush=True)
    else:
        print("  No issues found", flush=True)

    print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
