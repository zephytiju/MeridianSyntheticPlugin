# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping, Sequence

import pytest
from conftest import FakeMeridian, resource

from meridian_storage import Expression, ResourceRef
from meridian_storage.plugins.synthetic import (
    IMPLEMENTATION_COORDINATE,
    CollectionSpec,
    ExceededBudget,
    ExecutionBounds,
    FieldGeneratorSpec,
    Generator,
    ImplementationPin,
    MemorySink,
    MeridianEvidenceHook,
    MeridianSourceReader,
    OutputMode,
    RunEvidence,
    SourcePolicyViolation,
    SourceSpec,
    SyntheticSpec,
    TimeBounds,
)
from meridian_storage.plugins.synthetic.execution.sources import (
    IDENTITY_TRANSFORMATION_DIGEST,
    SourceTransformation,
    SourceTransformationRegistry,
    builtin_source_transformations,
    materialize_sources,
)
from meridian_storage.semantics import (
    CatalogName,
    FieldDefinition,
    LogicalKind,
    LogicalType,
    ResourceReference,
    SchemaDocument,
    SchemaReference,
    SemanticKind,
)


def source_spec() -> SourceSpec:
    source_resource = resource("source-cities")
    query = Expression(
        "structured",
        "query",
        {"resource": source_resource.to_dict(), "select": ["city"], "limit": 10},
    )
    return SourceSpec(
        "cities",
        source_resource,
        query,
        ("city",),
        "core.identity@1",
        IDENTITY_TRANSFORMATION_DIGEST,
        10,
    )


def source_backed_spec() -> SyntheticSpec:
    schema = SchemaDocument(
        SchemaReference(CatalogName.STRUCTURED, "fixtures", "people", "1.0.0"),
        SemanticKind.RELATIONAL,
        (
            FieldDefinition("id", LogicalType(LogicalKind.INT64)),
            FieldDefinition("city", LogicalType(LogicalKind.STRING)),
        ),
        ("id",),
    )
    collection = CollectionSpec(
        "people",
        ResourceReference(CatalogName.STRUCTURED, "fixtures", "people"),
        schema,
        4,
        fields={"city": FieldGeneratorSpec("core.source-field@1", {"field": "city"})},
        source_id="cities",
    )
    return SyntheticSpec(
        "source-suite",
        "1.0.0",
        1,
        ("en",),
        TimeBounds("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"),
        ExecutionBounds(10, 100_000, 10_000, 100_000, 2),
        ImplementationPin(IMPLEMENTATION_COORDINATE),
        OutputMode.RECORDS,
        (collection,),
        sources=(source_spec(),),
    )


def test_source_reader_and_evidence_exclude_raw_values(operation_context) -> None:
    runtime = FakeMeridian(query_rows=[{"city": "Paris"}, {"city": "Tokyo"}])
    reader = MeridianSourceReader(runtime, operation_context)  # type: ignore[arg-type]
    sink = MemorySink()
    run = Generator(source_backed_spec()).run(sink, source_reader=reader)
    assert [item.values["city"] for item in sink.records(run.run_id)] == [
        "Paris",
        "Tokyo",
        "Paris",
        "Tokyo",
    ]
    evidence_text = repr(run.source_evidence)
    assert "Paris" not in evidence_text
    assert "Tokyo" not in evidence_text
    assert run.source_evidence[0]["rowCount"] == 2
    assert runtime.expressions[0].method == "query"


def test_source_boundaries_and_transform_pins_are_enforced() -> None:
    with pytest.raises(SourcePolicyViolation, match="SourceReader"):
        Generator(source_backed_spec()).run(MemorySink())

    class BadReader:
        def read(self, source: SourceSpec) -> Sequence[Mapping[str, object]]:
            return ({"city": "Paris", "secret": "value"},)

    with pytest.raises(SourcePolicyViolation, match="projection"):
        Generator(source_backed_spec()).run(MemorySink(), source_reader=BadReader())

    class LargeReader:
        def read(self, source: SourceSpec) -> Sequence[Mapping[str, object]]:
            return ({"city": "a value larger than one byte"},)

    with pytest.raises(ExceededBudget, match="maxMemoryBytes"):
        materialize_sources(
            (source_spec(),),
            reader=LargeReader(),
            transformations=builtin_source_transformations(),
            max_memory_bytes=1,
        )

    registry = SourceTransformationRegistry()
    registry.register(
        SourceTransformation("core.identity@1", "sha256:" + "0" * 64, lambda row, ordinal: row)
    )
    with pytest.raises(SourcePolicyViolation, match="digest pin"):
        materialize_sources((source_spec(),), reader=BadReader(), transformations=registry)


def test_source_registry_rejects_duplicates() -> None:
    sealed = builtin_source_transformations()
    with pytest.raises(RuntimeError, match="sealed"):
        sealed.register(
            SourceTransformation("other@1", "sha256:" + "1" * 64, lambda row, ordinal: row)
        )
    registry = SourceTransformationRegistry()
    transformation = SourceTransformation(
        "core.identity@1", IDENTITY_TRANSFORMATION_DIGEST, lambda row, ordinal: row
    )
    registry.register(transformation)
    with pytest.raises(SourcePolicyViolation, match="duplicate"):
        registry.register(transformation)


def test_meridian_evidence_hook_appends_lineage(operation_context) -> None:
    runtime = FakeMeridian()
    hook = MeridianEvidenceHook(
        runtime,  # type: ignore[arg-type]
        operation_context,
        ResourceRef("evidence", "fixtures", "synthetic-lineage"),
    )
    run = Generator(source_backed_spec()).run(
        MemorySink(),
        source_reader=type(
            "Reader",
            (),
            {"read": lambda self, source: ({"city": "Paris"},)},
        )(),
        evidence_hook=hook,
    )
    evidence_expressions = [item for item in runtime.expressions if item.catalog == "evidence"]
    assert len(evidence_expressions) == 4
    assert all(item.method == "append" for item in evidence_expressions)
    serialized = repr([item.to_dict() for item in evidence_expressions])
    assert "Paris" not in serialized
    assert run.state.value == "PUBLISHED"


def test_run_evidence_rejects_raw_source_fields() -> None:
    spec = source_backed_spec()
    with pytest.raises(ValueError, match="source evidence"):
        RunEvidence(
            run_id="run",
            state="RUNNING",
            spec_fingerprint=spec.fingerprint,
            implementation_digest="sha256:" + "1" * 64,
            schema_fingerprints=spec.schema_fingerprints,
            source_evidence=({"rawValue": "must-not-appear"},),
        )
    with pytest.raises(ValueError, match="state"):
        RunEvidence(
            run_id="run",
            state="UNKNOWN",
            spec_fingerprint=spec.fingerprint,
            implementation_digest="sha256:" + "1" * 64,
            schema_fingerprints=spec.schema_fingerprints,
        )
