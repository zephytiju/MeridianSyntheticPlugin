# SPDX-License-Identifier: Apache-2.0
"""Safe run evidence and the public Meridian Evidence append hook."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Protocol, cast, runtime_checkable

from meridian_storage import Expression, Meridian, OperationContext, OperationResult, ResourceRef
from meridian_storage.evidence import EvidenceReference, LineageRecord, LineageStatus

from .canonical import (
    JsonValue,
    bounded_token,
    freeze_source_evidence,
    require_digest,
    sha256_digest,
)


@dataclass(frozen=True, slots=True)
class RunEvidence:
    run_id: str
    state: str
    spec_fingerprint: str
    implementation_digest: str
    schema_fingerprints: Mapping[str, str]
    partition_digests: Mapping[str, str] = field(default_factory=dict)
    query_fingerprints: tuple[str, ...] = ()
    source_evidence: tuple[Mapping[str, JsonValue], ...] = ()
    validation_digest: str | None = None
    artifact_digest: str | None = None
    error_code: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", bounded_token(self.run_id, "run id", 256))
        if self.state not in {
            "PLANNED",
            "RUNNING",
            "VALIDATING",
            "PUBLISHED",
            "FAILED",
            "CANCELLED",
        }:
            raise ValueError("unsupported synthetic run evidence state")
        object.__setattr__(self, "spec_fingerprint", require_digest(self.spec_fingerprint, "spec"))
        object.__setattr__(
            self,
            "implementation_digest",
            require_digest(self.implementation_digest, "implementation"),
        )
        for name in ("validation_digest", "artifact_digest"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, require_digest(value, name))
        object.__setattr__(
            self,
            "schema_fingerprints",
            MappingProxyType(
                {
                    name: require_digest(value, f"Schema {name}")
                    for name, value in sorted(self.schema_fingerprints.items())
                }
            ),
        )
        object.__setattr__(
            self,
            "partition_digests",
            MappingProxyType(
                {
                    name: require_digest(value, f"partition {name}")
                    for name, value in sorted(self.partition_digests.items())
                }
            ),
        )
        object.__setattr__(
            self,
            "query_fingerprints",
            tuple(
                sorted(
                    require_digest(value, "query fingerprint") for value in self.query_fingerprints
                )
            ),
        )
        object.__setattr__(
            self,
            "source_evidence",
            freeze_source_evidence(self.source_evidence),
        )

    def to_dict(self) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {
            "runId": self.run_id,
            "state": self.state,
            "specFingerprint": self.spec_fingerprint,
            "implementationDigest": self.implementation_digest,
            "schemaFingerprints": dict(self.schema_fingerprints),
            "partitionDigests": dict(self.partition_digests),
            "queryFingerprints": list(self.query_fingerprints),
            "sources": [dict(item) for item in self.source_evidence],
        }
        if self.validation_digest is not None:
            result["validationDigest"] = self.validation_digest
        if self.artifact_digest is not None:
            result["artifactDigest"] = self.artifact_digest
        if self.error_code is not None:
            result["errorCode"] = self.error_code
        return result


@runtime_checkable
class EvidenceHook(Protocol):
    def emit(self, evidence: RunEvidence) -> None: ...


class CollectingEvidenceHook:
    def __init__(self) -> None:
        self.events: list[RunEvidence] = []

    def emit(self, evidence: RunEvidence) -> None:
        self.events.append(evidence)


class _EvidenceSurface(Protocol):
    def append(
        self,
        *,
        resource: ResourceRef | str | Mapping[str, object],
        data: Mapping[str, object] | LineageRecord,
        profile: str | None = None,
        idempotency_key: str | None = None,
        require_atomic: bool = False,
    ) -> Expression: ...


class _Runtime(Protocol):
    def catalog(self, name: str) -> object: ...

    def context(self, context: OperationContext) -> AbstractContextManager[OperationContext]: ...

    def execute(self, expression: Expression) -> OperationResult: ...


class MeridianEvidenceHook:
    """Append lineage through the released ``evidence`` Catalog only."""

    def __init__(
        self,
        meridian: Meridian,
        context: OperationContext,
        resource: ResourceRef | str | Mapping[str, object],
    ) -> None:
        self._meridian = cast(_Runtime, meridian)
        self._context = context
        self._resource = resource

    def emit(self, evidence: RunEvidence) -> None:
        status = {
            "PLANNED": LineageStatus.STARTED,
            "RUNNING": LineageStatus.STARTED,
            "VALIDATING": LineageStatus.CHECKPOINT,
            "PUBLISHED": LineageStatus.COMPLETED,
            "FAILED": LineageStatus.FAILED,
            "CANCELLED": LineageStatus.FAILED,
        }[evidence.state]
        inputs = tuple(
            EvidenceReference("query", item, digest=item) for item in evidence.query_fingerprints
        )
        record = LineageRecord(
            activity="meridian.synthetic.generate",
            status=status,
            execution_id=evidence.run_id,
            inputs=inputs,
            schema_versions=evidence.schema_fingerprints,
            query_fingerprints=evidence.query_fingerprints,
            transformation_digest=evidence.implementation_digest,
            artifact_digest=evidence.artifact_digest,
            checkpoint_key=(evidence.state if status is LineageStatus.CHECKPOINT else None),
            context_scope=dict(self._context.scope),
            metadata_values=evidence.to_dict(),
        )
        key = (
            f"synthetic:{evidence.state.lower()}:"
            f"{sha256_digest(cast(JsonValue, evidence.to_dict())).removeprefix('sha256:')}"
        )
        surface = cast(_EvidenceSurface, self._meridian.catalog("evidence"))
        expression = surface.append(
            resource=self._resource,
            data=record,
            profile="lineage",
            idempotency_key=key,
            require_atomic=True,
        )
        with self._meridian.context(replace(self._context, idempotency_key=key)):
            self._meridian.execute(expression)


__all__ = [
    "CollectingEvidenceHook",
    "EvidenceHook",
    "MeridianEvidenceHook",
    "RunEvidence",
]
