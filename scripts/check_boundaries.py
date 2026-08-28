#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Fail when source or metadata crosses the locked Meridian V1 boundaries."""

from __future__ import annotations

import ast
import json
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_MODULES = {
    "boto3",
    "botocore",
    "confluent_kafka",
    "kafka",
    "pymongo",
    "psycopg",
    "redis",
    "sqlalchemy",
}
EXPECTED_RUNTIME = {
    "meridian-storage-core==1.0.0",
    "meridian-storage-evidence==1.0.0",
    "meridian-storage-query==1.0.0",
    "meridian-storage-semantics==1.0.0",
    "meridian-storage-streaming==1.0.0",
}
ALLOWED_CATALOGS = {"structured", "evidence", "streaming"}
EXPECTED_DISTRIBUTION = "meridian-storage-plugin-synthetic"
EXPECTED_VERSION = "1.0.1"
EXPECTED_ENTRY_POINTS = {
    "meridian_storage.plugins": {
        "synthetic": "meridian_storage.plugins.synthetic.plugin:SyntheticPluginFactory"
    },
    "meridian_storage.schemas": {
        "synthetic": "meridian_storage.plugins.synthetic.schema:SyntheticSchemaProvider"
    },
}


def imported_roots(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    result: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            result.add(node.module.split(".", 1)[0])
    return result


def main() -> int:
    failures: list[str] = []
    for path in sorted((ROOT / "src").rglob("*.py")):
        forbidden = imported_roots(path) & FORBIDDEN_MODULES
        if forbidden:
            failures.append(f"{path.relative_to(ROOT)} imports {sorted(forbidden)!r}")
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    if project["name"] != EXPECTED_DISTRIBUTION or project["version"] != EXPECTED_VERSION:
        failures.append("project distribution identity differs from the canonical release")
    if project.get("entry-points") != EXPECTED_ENTRY_POINTS:
        failures.append("project entry points differ from the canonical plugin contract")
    dependencies = set(project["dependencies"])
    if dependencies != EXPECTED_RUNTIME:
        failures.append(
            f"runtime dependencies differ: expected={sorted(EXPECTED_RUNTIME)!r}, "
            f"actual={sorted(dependencies)!r}"
        )
    compatibility = json.loads((ROOT / "compatibility.json").read_text())
    if (
        compatibility["package"] != EXPECTED_DISTRIBUTION
        or compatibility["version"] != EXPECTED_VERSION
    ):
        failures.append("compatibility identity differs from project metadata")
    if compatibility["catalogsOwned"]:
        failures.append("synthetic plugin must not own a Catalog")
    if set(compatibility["catalogsUsed"]) != ALLOWED_CATALOGS:
        failures.append("Catalog usage differs from structured/evidence/streaming")
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print("boundary-check: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
