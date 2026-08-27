#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Create a deterministic SPDX 2.3 package SBOM for a built wheel."""

from __future__ import annotations

import argparse
import hashlib
import json
import zipfile
from pathlib import Path


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def wheel_metadata(path: Path) -> tuple[str, str, list[str]]:
    with zipfile.ZipFile(path) as archive:
        metadata_name = next(
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        )
        lines = archive.read(metadata_name).decode().splitlines()
    name = next(line.split(": ", 1)[1] for line in lines if line.startswith("Name: "))
    version = next(line.split(": ", 1)[1] for line in lines if line.startswith("Version: "))
    dependencies = sorted(
        line.split(": ", 1)[1].split(";", 1)[0].strip()
        for line in lines
        if line.startswith("Requires-Dist: ") and "extra ==" not in line
    )
    return name, version, dependencies


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel", type=Path)
    parser.add_argument("output", type=Path)
    arguments = parser.parse_args()
    name, version, dependencies = wheel_metadata(arguments.wheel)
    document = {
        "SPDXID": "SPDXRef-DOCUMENT",
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "name": f"{name}-{version}",
        "documentNamespace": (
            f"https://github.com/zephytiju/MeridianSyntheticPlugin/"
            f"releases/{version}/{digest(arguments.wheel)}"
        ),
        "creationInfo": {
            "created": "2026-08-26T00:00:00Z",
            "creators": ["Tool: meridian-plugin-synthetic/scripts/build_sbom.py"],
        },
        "packages": [
            {
                "SPDXID": "SPDXRef-Package",
                "name": name,
                "versionInfo": version,
                "downloadLocation": "NOASSERTION",
                "filesAnalyzed": False,
                "licenseConcluded": "Apache-2.0",
                "licenseDeclared": "Apache-2.0",
                "checksums": [{"algorithm": "SHA256", "checksumValue": digest(arguments.wheel)}],
                "externalRefs": [
                    {
                        "referenceCategory": "PACKAGE-MANAGER",
                        "referenceType": "purl",
                        "referenceLocator": f"pkg:pypi/{name}@{version}",
                    }
                ],
                "comment": "Runtime requirements: " + ", ".join(dependencies),
            }
        ],
        "relationships": [
            {
                "spdxElementId": "SPDXRef-DOCUMENT",
                "relationshipType": "DESCRIBES",
                "relatedSpdxElement": "SPDXRef-Package",
            }
        ],
    }
    arguments.output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
