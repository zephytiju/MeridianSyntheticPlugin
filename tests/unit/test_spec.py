# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import copy
import json

import pytest
from conftest import basic_schema, basic_spec, resource

from meridian_storage import Expression
from meridian_storage.plugins.synthetic import (
    IMPLEMENTATION_COORDINATE,
    CollectionSpec,
    ExecutionBounds,
    ImplementationPin,
    InvalidSpec,
    OutputMode,
    SourcePolicyViolation,
    SourceSpec,
    SyntheticSpec,
    TimeBounds,
)
from meridian_storage.plugins.synthetic.execution.sources import (
    IDENTITY_TRANSFORMATION_DIGEST,
)


def test_spec_round_trip_and_fingerprint() -> None:
    spec = basic_spec()
    mapping = spec.to_dict()
    loaded = SyntheticSpec.load(json.dumps(mapping))
    assert loaded.to_dict() == mapping
    assert loaded.fingerprint == spec.fingerprint
    assert loaded.schema_fingerprints == {"users": basic_schema().fingerprint}


@pytest.mark.parametrize(
    ("mutator", "match"),
    [
        (lambda value: value.update({"unknown": True}), "unknown or missing"),
        (lambda value: value.update({"outputMode": "service"}), "outputMode"),
        (lambda value: value.update({"locales": []}), "locales"),
        (lambda value: value["bounds"].update({"maxRecords": 1}), "maxRecords"),
    ],
)
def test_spec_rejects_invalid_documents(mutator, match: str) -> None:
    value = copy.deepcopy(basic_spec().to_dict())
    mutator(value)
    with pytest.raises(InvalidSpec, match=match):
        SyntheticSpec.load(value)


def test_spec_rejects_cycles_and_schema_fingerprint_mismatch() -> None:
    schema = basic_schema()
    first = CollectionSpec("first", resource("users"), schema, 1, depends_on=("second",))
    second = CollectionSpec("second", resource("users"), schema, 1, depends_on=("first",))
    with pytest.raises(InvalidSpec, match="cycle"):
        SyntheticSpec(
            "cycle",
            "1.0.0",
            1,
            ("en",),
            TimeBounds("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"),
            ExecutionBounds(10, 10000, 1000, 10000, 1),
            ImplementationPin(IMPLEMENTATION_COORDINATE),
            OutputMode.RECORDS,
            (first, second),
        )
    value = basic_spec().to_dict()
    value["collections"][0]["schema"]["fingerprint"] = "sha256:" + "0" * 64
    with pytest.raises(InvalidSpec, match="fingerprint"):
        SyntheticSpec.load(value)


def test_source_policy_accepts_only_explicit_projected_query() -> None:
    query = Expression(
        "structured",
        "query",
        {"resource": resource("source").to_dict(), "select": ["city"], "limit": 5},
    )
    source = SourceSpec(
        "cities",
        resource("source"),
        query,
        ("city",),
        "core.identity@1",
        IDENTITY_TRANSFORMATION_DIGEST,
        5,
    )
    assert source.query_fingerprint == query.fingerprint
    with pytest.raises(SourcePolicyViolation, match="select differs"):
        SourceSpec(
            "cities",
            resource("source"),
            query,
            ("country",),
            "core.identity@1",
            IDENTITY_TRANSFORMATION_DIGEST,
            5,
        )
