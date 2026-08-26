# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import threading

import pytest
from conftest import basic_spec, relation_spec

from meridian_storage.plugins.synthetic import (
    IMPLEMENTATION_COORDINATE,
    CancellationToken,
    CancelledRun,
    CollectionSpec,
    CorrelationSpec,
    ExceededBudget,
    ExecutionBounds,
    FieldGeneratorSpec,
    Generator,
    ImplementationPin,
    InvalidSpec,
    MemorySink,
    OutputMode,
    PartitionReceipt,
    RunState,
    SinkConflict,
    SinkKind,
    SyntheticSpec,
    TimeBounds,
    ValidationFailure,
    ValidationRuleSpec,
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


def _wire(records) -> list[dict[str, object]]:
    return [item.to_dict() for item in records]


def test_generation_is_independent_of_run_workers_and_partitions() -> None:
    left_sink = MemorySink()
    right_sink = MemorySink()
    left = Generator(basic_spec(batch_size=2)).run(left_sink, run_id="left-run", workers=1)
    right = Generator(basic_spec(batch_size=5)).run(right_sink, run_id="right-run", workers=4)
    assert _wire(left_sink.records(left.run_id)) == _wire(right_sink.records(right.run_id))
    assert left.state is RunState.PUBLISHED
    assert left.validation.passed
    assert left.progress.generated_records == 9
    assert left.evidence[-1].validation_digest == left.validation.digest


def test_default_run_id_and_receipts_are_reproducible() -> None:
    spec = basic_spec()
    first_sink = MemorySink()
    second_sink = MemorySink()
    first = Generator(spec).run(first_sink)
    second = Generator(spec).run(second_sink)
    assert first.run_id == second.run_id
    assert [item.digest for item in first.receipts] == [item.digest for item in second.receipts]


def test_relation_records_reference_existing_endpoints() -> None:
    sink = MemorySink()
    run = Generator(relation_spec()).run(sink, workers=4)
    accounts = {item.record_id for item in sink.records(run.run_id, "accounts")}
    orders = {item.record_id for item in sink.records(run.run_id, "orders")}
    edges = sink.records(run.run_id, "ownership")
    assert len(edges) == 11
    assert all(item.values["source"]["recordId"] in accounts for item in edges)
    assert all(item.values["target"]["recordId"] in orders for item in edges)
    assert run.validation.to_dict()["passed"] is True


def test_correlations_and_custom_validation() -> None:
    resource = ResourceReference(CatalogName.STRUCTURED, "fixtures", "metrics")
    schema = SchemaDocument(
        SchemaReference(CatalogName.STRUCTURED, "fixtures", "metrics", "1.0.0"),
        SemanticKind.RELATIONAL,
        (
            FieldDefinition("id", LogicalType(LogicalKind.INT64)),
            FieldDefinition("base", LogicalType(LogicalKind.FLOAT64)),
            FieldDefinition("scaled", LogicalType(LogicalKind.FLOAT64)),
        ),
        ("id",),
    )
    collection = CollectionSpec(
        "metrics",
        resource,
        schema,
        5,
        fields={
            "base": FieldGeneratorSpec("core.sequence@1", {"start": 2, "step": 2}),
        },
        correlations=(CorrelationSpec("scaled", "linear", ("base",), {"slope": 3}),),
    )
    spec = SyntheticSpec(
        "correlations",
        "1.0.0",
        5,
        ("en",),
        TimeBounds("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"),
        ExecutionBounds(10, 100_000, 10_000, 100_000, 2),
        ImplementationPin(IMPLEMENTATION_COORDINATE),
        OutputMode.RECORDS,
        (collection,),
        validation_rules=(
            ValidationRuleSpec("count", "count", "metrics", {"equals": 5}),
            ValidationRuleSpec("unique-id", "unique", "metrics.id"),
            ValidationRuleSpec("range", "range", "metrics.scaled", {"min": 6, "max": 30}),
        ),
    )
    sink = MemorySink()
    run = Generator(spec).run(sink)
    assert [item.values["scaled"] for item in sink.records(run.run_id)] == [
        6.0,
        12.0,
        18.0,
        24.0,
        30.0,
    ]
    assert run.validation.passed


def test_blocking_validation_failure_aborts() -> None:
    spec = basic_spec()
    failing = SyntheticSpec(
        spec.spec_id,
        spec.version,
        spec.seed,
        spec.locales,
        spec.time_bounds,
        spec.bounds,
        spec.implementation,
        spec.output_mode,
        spec.collections,
        validation_rules=(ValidationRuleSpec("wrong-count", "count", "users", {"equals": 100}),),
    )
    with pytest.raises(ValidationFailure):
        Generator(failing).run(MemorySink())


def test_cancellation_and_invalid_sink_are_rejected() -> None:
    cancellation = CancellationToken()
    cancellation.cancel()
    with pytest.raises(CancelledRun):
        Generator(basic_spec()).run(MemorySink(), cancellation=cancellation)
    with pytest.raises(InvalidSpec, match="sink kind"):
        Generator(basic_spec(output_mode=OutputMode.DATASET)).run(MemorySink())


def test_cancellation_token_is_thread_safe() -> None:
    token = CancellationToken()
    thread = threading.Thread(target=token.cancel)
    thread.start()
    thread.join()
    assert token.cancelled


def test_implementation_coordinate_and_digest_are_enforced() -> None:
    spec = basic_spec()
    wrong_coordinate = SyntheticSpec(
        spec.spec_id,
        spec.version,
        spec.seed,
        spec.locales,
        spec.time_bounds,
        spec.bounds,
        ImplementationPin("another-package@1.0.0"),
        spec.output_mode,
        spec.collections,
    )
    with pytest.raises(InvalidSpec, match="another package"):
        Generator(wrong_coordinate)
    wrong_digest = SyntheticSpec(
        spec.spec_id,
        spec.version,
        spec.seed,
        spec.locales,
        spec.time_bounds,
        spec.bounds,
        ImplementationPin(IMPLEMENTATION_COORDINATE, "sha256:" + "0" * 64),
        spec.output_mode,
        spec.collections,
    )
    with pytest.raises(InvalidSpec, match="digest"):
        Generator(wrong_digest)


def test_output_budget_is_checked_before_sink_side_effects() -> None:
    spec = basic_spec(count=1, batch_size=1)
    bounded = SyntheticSpec(
        spec.spec_id,
        spec.version,
        spec.seed,
        spec.locales,
        spec.time_bounds,
        ExecutionBounds(10, 100, 10_000, 800, 1),
        spec.implementation,
        spec.output_mode,
        spec.collections,
    )
    sink = MemorySink()
    with pytest.raises(ExceededBudget):
        Generator(bounded).run(sink, run_id="bounded-run")
    assert sink.records("bounded-run") == ()


def test_sink_receipts_are_verified() -> None:
    class DishonestSink:
        kind = SinkKind.MEMORY

        def completed_partitions(self, run_id):
            return frozenset()

        def write_partition(self, run_id, partition):
            return PartitionReceipt(
                partition.collection_id,
                partition.partition_id,
                len(partition.records),
                1,
                "sha256:" + "0" * 64,
            )

        def finalize(self, run_id, receipts, schemas):
            return {}

        def abort(self, run_id, receipts, reason):
            return None

    with pytest.raises(SinkConflict, match="receipt"):
        Generator(basic_spec(count=1)).run(DishonestSink())
