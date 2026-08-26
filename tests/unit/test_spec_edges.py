# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import copy

import pytest
from conftest import basic_schema, basic_spec, relation_spec, resource

from meridian_storage import Expression
from meridian_storage.plugins.synthetic import (
    CollectionSpec,
    CorrelationSpec,
    ExecutionBounds,
    FieldGeneratorSpec,
    ImplementationPin,
    InvalidSpec,
    RelationSpec,
    SourcePolicyViolation,
    SourceSpec,
    SyntheticSpec,
    TimeBounds,
    ValidationRuleSpec,
)
from meridian_storage.plugins.synthetic.execution.sources import (
    IDENTITY_TRANSFORMATION_DIGEST,
)
from meridian_storage.semantics import ResourceReference


@pytest.mark.parametrize(
    "value",
    [
        ("bad", "2026-01-01T00:00:00Z"),
        ("2026-01-01", "2026-01-02T00:00:00Z"),
        ("2026-01-02T00:00:00Z", "2026-01-01T00:00:00Z"),
    ],
)
def test_invalid_time_bounds(value) -> None:
    with pytest.raises(InvalidSpec):
        TimeBounds(*value)


@pytest.mark.parametrize(
    "values",
    [
        (0, 1, 1, 1, 1),
        (1, 10, 1, 10, 2),
        (10, 10, 1, 1000, 1),
    ],
)
def test_invalid_execution_bounds(values) -> None:
    with pytest.raises(InvalidSpec):
        ExecutionBounds(*values)


def test_field_and_correlation_specs_reject_invalid_values() -> None:
    with pytest.raises(InvalidSpec, match="nullRate"):
        FieldGeneratorSpec("core.string@1", null_rate=2)
    with pytest.raises(ValueError, match="fingerprint"):
        FieldGeneratorSpec("core.string@1", expected_digest="bad")
    with pytest.raises(InvalidSpec, match="unsupported"):
        CorrelationSpec("name", "random", ("id",))
    with pytest.raises(InvalidSpec, match="non-empty"):
        CorrelationSpec("name", "copy", ())
    with pytest.raises(InvalidSpec, match="non-empty"):
        CorrelationSpec("name", "copy", ("name",))


def test_collection_spec_rejects_mismatches() -> None:
    schema = basic_schema()
    with pytest.raises(InvalidSpec, match="Namespace"):
        CollectionSpec(
            "users",
            ResourceReference.parse("structured:other.users"),
            schema,
            1,
        )
    with pytest.raises(InvalidSpec, match="positive"):
        CollectionSpec("users", resource("users"), schema, 0)
    with pytest.raises(InvalidSpec, match="unknown"):
        CollectionSpec(
            "users",
            resource("users"),
            schema,
            1,
            fields={"missing": FieldGeneratorSpec("core.string@1")},
        )
    correlation = CorrelationSpec("name", "copy", ("id",))
    with pytest.raises(InvalidSpec, match="unique"):
        CollectionSpec(
            "users",
            resource("users"),
            schema,
            1,
            correlations=(correlation, correlation),
        )
    with pytest.raises(InvalidSpec, match="unknown fields"):
        CollectionSpec(
            "users",
            resource("users"),
            schema,
            1,
            correlations=(CorrelationSpec("name", "copy", ("missing",)),),
        )
    with pytest.raises(InvalidSpec, match="dependencies"):
        CollectionSpec("users", resource("users"), schema, 1, depends_on=("users",))


def test_relation_spec_requires_profile_and_matching_endpoints() -> None:
    plain = CollectionSpec("users", resource("users"), basic_schema(), 1)
    with pytest.raises(InvalidSpec, match="Relation profile"):
        RelationSpec(plain, "users", "users", "source", "target")
    relation = relation_spec().relations[0]
    with pytest.raises(InvalidSpec, match="disagree"):
        RelationSpec(
            relation.collection,
            relation.source_collection,
            relation.target_collection,
            "target",
            "source",
        )


def test_source_spec_rejects_unsafe_queries() -> None:
    source_resource = resource("source")
    base = {
        "resource": source_resource.to_dict(),
        "select": ["city"],
        "limit": 1,
    }
    with pytest.raises(SourcePolicyViolation, match="only structured.query"):
        SourceSpec(
            "source",
            source_resource,
            Expression("structured", "put", base),
            ("city",),
            "core.identity@1",
            IDENTITY_TRANSFORMATION_DIGEST,
            1,
        )
    with pytest.raises(SourcePolicyViolation, match="Resource differs"):
        SourceSpec(
            "source",
            source_resource,
            Expression(
                "structured",
                "query",
                {**base, "resource": resource("other").to_dict()},
            ),
            ("city",),
            "core.identity@1",
            IDENTITY_TRANSFORMATION_DIGEST,
            1,
        )
    with pytest.raises(SourcePolicyViolation, match="explicit"):
        SourceSpec(
            "source",
            source_resource,
            Expression("structured", "query", {**base, "select": []}),
            ("city",),
            "core.identity@1",
            IDENTITY_TRANSFORMATION_DIGEST,
            1,
        )
    with pytest.raises(SourcePolicyViolation, match="direct field"):
        SourceSpec(
            "source",
            source_resource,
            Expression("structured", "query", {**base, "select": [{"kind": "literal"}]}),
            ("city",),
            "core.identity@1",
            IDENTITY_TRANSFORMATION_DIGEST,
            1,
        )
    with pytest.raises(SourcePolicyViolation, match="positive"):
        SourceSpec(
            "source",
            source_resource,
            Expression("structured", "query", base),
            ("city",),
            "core.identity@1",
            IDENTITY_TRANSFORMATION_DIGEST,
            0,
        )


def test_synthetic_spec_rejects_global_invariants() -> None:
    spec = basic_spec()
    base = (
        spec.spec_id,
        spec.version,
        spec.seed,
        spec.locales,
        spec.time_bounds,
        spec.bounds,
        spec.implementation,
        spec.output_mode,
    )
    with pytest.raises(InvalidSpec, match="formatVersion"):
        SyntheticSpec(*base, spec.collections, format_version="v2")
    with pytest.raises(InvalidSpec, match="semantic version"):
        SyntheticSpec(base[0], "latest", *base[2:], spec.collections)
    with pytest.raises(InvalidSpec, match="seed"):
        SyntheticSpec(base[0], base[1], True, *base[3:], spec.collections)
    with pytest.raises(InvalidSpec, match="at least one"):
        SyntheticSpec(*base, ())
    with pytest.raises(InvalidSpec, match="globally unique"):
        SyntheticSpec(*base, (spec.collections[0], spec.collections[0]))
    sourced = CollectionSpec("users", resource("users"), basic_schema(), 1, source_id="missing")
    with pytest.raises(InvalidSpec, match="unknown source"):
        SyntheticSpec(*base, (sourced,))
    too_small = ExecutionBounds(1, 100_000, 1000, 100_000, 1)
    with pytest.raises(InvalidSpec, match="maxRecords"):
        SyntheticSpec(*base[:5], too_small, *base[6:], spec.collections)


def test_parser_rejects_non_json_and_wrong_container_shapes() -> None:
    with pytest.raises(InvalidSpec, match="valid UTF-8 JSON"):
        SyntheticSpec.load("{")
    with pytest.raises(InvalidSpec, match="object"):
        SyntheticSpec.load([])  # type: ignore[arg-type]
    value = copy.deepcopy(basic_spec().to_dict())
    value["collections"] = "not-an-array"
    with pytest.raises(InvalidSpec, match="array"):
        SyntheticSpec.load(value)
    with pytest.raises(ValueError, match="fingerprint"):
        ImplementationPin("meridian-plugin-synthetic@1.0.0", "bad")
    with pytest.raises(InvalidSpec, match="boolean"):
        ValidationRuleSpec("rule", "count", blocking="yes")  # type: ignore[arg-type]
