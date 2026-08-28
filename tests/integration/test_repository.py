# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping

import pytest
from conftest import basic_spec

from meridian_storage import Expression, ResourceRef
from meridian_storage.plugins.synthetic import (
    InvalidSpec,
    RunEvidence,
    SinkConflict,
    SyntheticRepository,
    SyntheticResources,
    SyntheticSpec,
)


class MemoryExecutor:
    def __init__(self) -> None:
        self.records: dict[tuple[str, tuple[tuple[str, object], ...]], Mapping[str, object]] = {}
        self.expressions: list[Expression] = []

    @staticmethod
    def _identity(
        resource_name: str, value: Mapping[str, object]
    ) -> tuple[tuple[str, object], ...]:
        fields = ("specId", "specVersion") if resource_name == "specs" else ("runId", "state")
        return tuple((name, value[name]) for name in fields)

    def execute(self, expression: Expression):
        self.expressions.append(expression)
        resource = expression.arguments["resource"]
        assert isinstance(resource, Mapping)
        resource_name = resource["name"]
        assert isinstance(resource_name, str)
        if expression.method == "get":
            where = expression.arguments["where"]
            assert isinstance(where, Mapping)
            key = (resource_name, self._identity(resource_name, where))
            record = self.records.get(key)
            return type("Result", (), {"data": None if record is None else {"record": record}})()
        assert expression.method == "put"
        data = expression.arguments["data"]
        assert isinstance(data, Mapping)
        record = dict(data)
        key = (resource_name, self._identity(resource_name, record))
        self.records[key] = record
        return type("Result", (), {"data": {"record": record}})()


def run_evidence() -> RunEvidence:
    spec = basic_spec(count=2, batch_size=1)
    return RunEvidence(
        run_id="run-1",
        state="PUBLISHED",
        spec_fingerprint=spec.fingerprint,
        implementation_digest="sha256:" + "1" * 64,
        schema_fingerprints=spec.schema_fingerprints,
        partition_digests={"users-00000000": "sha256:" + "2" * 64},
        validation_digest="sha256:" + "3" * 64,
    )


def test_specs_and_run_evidence_round_trip_deterministically() -> None:
    executor = MemoryExecutor()
    repository = SyntheticRepository(executor)  # type: ignore[arg-type]
    spec = basic_spec(count=2, batch_size=1)
    assert repository.get_spec(spec.spec_id, spec.version) is None
    assert repository.get_run_evidence("missing", "PUBLISHED") is None

    persisted, replayed = repository.register_spec(spec)
    assert persisted.fingerprint == spec.fingerprint
    assert replayed is False
    assert repository.get_spec(spec.spec_id, spec.version) == spec
    persisted_again, replayed_again = repository.register_spec(spec)
    assert persisted_again == spec
    assert replayed_again is True

    evidence = run_evidence()
    recorded, evidence_replayed = repository.record_run_evidence(evidence)
    assert recorded == evidence
    assert evidence_replayed is False
    assert repository.get_run_evidence(evidence.run_id, evidence.state) == evidence
    _, evidence_replayed_again = repository.record_run_evidence(evidence)
    assert evidence_replayed_again is True
    assert {item.method for item in executor.expressions} == {"get", "put"}


def test_repository_rejects_identity_conflicts_and_incompatible_records() -> None:
    executor = MemoryExecutor()
    repository = SyntheticRepository(executor)  # type: ignore[arg-type]
    spec = basic_spec(count=2, batch_size=1)
    repository.register_spec(spec)

    changed_document = spec.to_dict()
    changed_document["seed"] = "changed"
    changed = SyntheticSpec.load(changed_document)
    with pytest.raises(SinkConflict, match="different content"):
        repository.register_spec(changed)

    key = ("specs", (("specId", spec.spec_id), ("specVersion", spec.version)))
    corrupted = dict(executor.records[key])
    corrupted["fingerprint"] = "sha256:" + "0" * 64
    executor.records[key] = corrupted
    with pytest.raises(InvalidSpec, match="incompatible"):
        repository.get_spec(spec.spec_id, spec.version)

    evidence = run_evidence()
    repository.record_run_evidence(evidence)
    changed_evidence = RunEvidence(
        run_id=evidence.run_id,
        state=evidence.state,
        spec_fingerprint=evidence.spec_fingerprint,
        implementation_digest="sha256:" + "9" * 64,
        schema_fingerprints=evidence.schema_fingerprints,
    )
    with pytest.raises(SinkConflict, match="different content"):
        repository.record_run_evidence(changed_evidence)


def test_resources_reject_non_structured_and_overlapping_targets() -> None:
    with pytest.raises(InvalidSpec, match="structured Resource"):
        SyntheticResources(specs=ResourceRef("evidence", "synthetic", "specs"))
    shared = ResourceRef("structured", "synthetic", "shared")
    with pytest.raises(InvalidSpec, match="distinct"):
        SyntheticResources(specs=shared, run_evidence=shared)
