# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from conftest import FakeMeridian, basic_spec

from meridian_storage import ResourceRef, SchemaRef
from meridian_storage.plugins.synthetic import (
    DirectWritePolicy,
    Generator,
    MeridianRecordSink,
    MeridianStreamingSink,
    SinkConflict,
    TargetClass,
)


def test_record_sink_uses_only_public_structured_put(operation_context) -> None:
    runtime = FakeMeridian()
    sink = MeridianRecordSink(
        runtime,  # type: ignore[arg-type]
        operation_context,
        policy=DirectWritePolicy(target_class=TargetClass.DISPOSABLE),
    )
    run = Generator(basic_spec(count=5, batch_size=2)).run(sink)
    assert run.progress.generated_records == 5
    assert {item.catalog for item in runtime.expressions} == {"structured"}
    assert {item.method for item in runtime.expressions} == {"put"}
    assert all(context.tenant == "tenant-a" for context in runtime.contexts)
    assert all(context.idempotency_key for context in runtime.contexts)


def test_record_sink_resume_skips_completed_partitions(operation_context) -> None:
    runtime = FakeMeridian()
    sink = MeridianRecordSink(
        runtime,  # type: ignore[arg-type]
        operation_context,
        policy=DirectWritePolicy(),
    )
    generator = Generator(basic_spec(count=4, batch_size=2))
    first = generator.run(sink, run_id="resume-run")
    calls = len(runtime.expressions)
    second = generator.run(sink, run_id="resume-run")
    assert len(runtime.expressions) == calls
    assert all(item.skipped for item in second.receipts)
    assert [item.digest for item in first.receipts] == [item.digest for item in second.receipts]


def test_record_sink_can_disable_resume(operation_context) -> None:
    runtime = FakeMeridian()
    sink = MeridianRecordSink(
        runtime,  # type: ignore[arg-type]
        operation_context,
        policy=DirectWritePolicy(resume=False),
    )
    generator = Generator(basic_spec(count=4, batch_size=2))
    generator.run(sink, run_id="rewrite-run")
    calls = len(runtime.expressions)
    second = generator.run(sink, run_id="rewrite-run")
    assert len(runtime.expressions) == calls * 2
    assert all(not item.skipped for item in second.receipts)


def test_successful_staging_write_releases_cleanup_references(operation_context) -> None:
    runtime = FakeMeridian()
    sink = MeridianRecordSink(
        runtime,  # type: ignore[arg-type]
        operation_context,
        policy=DirectWritePolicy(
            target_class=TargetClass.STAGING,
            cleanup_on_failure=True,
        ),
    )
    result = Generator(basic_spec(count=2)).run(sink, run_id="staging-run")
    calls = len(runtime.expressions)
    sink.abort(result.run_id, result.receipts, "post-finalization check")
    assert len(runtime.expressions) == calls


def test_record_policy_requires_production_approval() -> None:
    with pytest.raises(ValueError, match="production"):
        DirectWritePolicy(target_class=TargetClass.PRODUCTION)
    approved = DirectWritePolicy(target_class=TargetClass.PRODUCTION, production_approved=True)
    assert approved.production_approved
    with pytest.raises(ValueError, match="staging"):
        DirectWritePolicy(cleanup_on_failure=True)
    with pytest.raises(TypeError, match="booleans"):
        DirectWritePolicy(resume="yes")  # type: ignore[arg-type]


def test_streaming_sink_uses_public_publish_batch(operation_context) -> None:
    runtime = FakeMeridian()
    sink = MeridianStreamingSink(
        runtime,  # type: ignore[arg-type]
        operation_context,
        streams={"users": ResourceRef("streaming", "fixtures", "user-events")},
        schemas={"users": SchemaRef("streaming", "fixtures", "user-event", "1.0.0")},
    )
    run = Generator(basic_spec(output_mode="streaming", count=7, batch_size=7)).run(sink)
    assert run.sink_result["events"] == 7
    assert {item.catalog for item in runtime.expressions} == {"streaming"}
    assert {item.method for item in runtime.expressions} == {"publish_batch"}
    expression = runtime.expressions[0]
    assert expression.arguments["idempotencyKey"].startswith(run.run_id)
    assert all("formatVersion" in item for item in expression.arguments["data"])


def test_streaming_sink_rejects_invalid_mapping(operation_context) -> None:
    runtime = FakeMeridian()
    with pytest.raises(ValueError, match="identical keys"):
        MeridianStreamingSink(
            runtime,  # type: ignore[arg-type]
            operation_context,
            streams={"users": "streaming:fixtures.users"},
            schemas={},
        )
    with pytest.raises(ValueError, match="Namespace"):
        MeridianStreamingSink(
            runtime,  # type: ignore[arg-type]
            operation_context,
            streams={"users": "streaming:fixtures.users"},
            schemas={
                "users": {
                    "catalog": "streaming",
                    "namespace": "other",
                    "name": "user-event",
                    "version": "1.0.0",
                }
            },
        )


class FailingMeridian(FakeMeridian):
    def execute(self, expression):
        raise RuntimeError("provider unavailable")


def test_sink_errors_are_wrapped(operation_context) -> None:
    sink = MeridianRecordSink(
        FailingMeridian(),  # type: ignore[arg-type]
        operation_context,
        policy=DirectWritePolicy(),
    )
    with pytest.raises(SinkConflict, match="structured write"):
        Generator(basic_spec(count=1)).run(sink)
