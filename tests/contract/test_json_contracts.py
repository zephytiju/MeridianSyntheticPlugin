# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
from pathlib import Path

from conftest import basic_spec
from jsonschema import Draft202012Validator, FormatChecker

from meridian_storage.plugins.synthetic import (
    CanonicalDatasetSink,
    Generator,
    InMemoryPartitionStore,
    InvalidSpec,
    OutputMode,
)

ROOT = Path(__file__).parents[2]


def _schema(name: str) -> dict[str, object]:
    return json.loads((ROOT / "contracts" / name).read_text())


def test_synthetic_spec_serialization_matches_published_schema() -> None:
    document = basic_spec().to_dict()
    Draft202012Validator(
        _schema("synthetic-spec.v1.schema.json"), format_checker=FormatChecker()
    ).validate(document)


def test_dataset_manifest_matches_published_schema() -> None:
    spec = basic_spec(output_mode=OutputMode.DATASET)
    sink = CanonicalDatasetSink(
        store=InMemoryPartitionStore(max_bytes=spec.bounds.max_memory_bytes)
    )
    manifest = Generator(spec).run(sink).manifest.to_dict()
    Draft202012Validator(_schema("dataset-manifest.v1.schema.json")).validate(manifest)


def test_error_envelope_matches_published_schema() -> None:
    error = InvalidSpec("invalid", field_path="$.id", details={"reason": "test"})
    Draft202012Validator(_schema("synthetic-error.v1.schema.json")).validate(error.to_dict())


def test_contract_files_are_valid_draft_2020_12_schemas() -> None:
    for path in sorted((ROOT / "contracts").glob("*.json")):
        Draft202012Validator.check_schema(json.loads(path.read_text()))
