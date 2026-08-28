# SPDX-License-Identifier: Apache-2.0
"""Meridian-backed persistence for Synthetic specifications and run evidence."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol, cast

from meridian_storage import (
    ConflictError,
    Expression,
    NotFoundError,
    OperationResult,
    ResourceRef,
)
from meridian_storage.semantics import StructuredCatalogSurface

from .canonical import JsonValue, sha256_digest
from .errors import InvalidSpec, SinkConflict
from .evidence import RunEvidence
from .spec import SyntheticSpec

SPEC_RESOURCE_FORMAT = "meridian.synthetic.spec-resource.v1"
RUN_EVIDENCE_RESOURCE_FORMAT = "meridian.synthetic.run-evidence-resource.v1"


class MeridianExecutor(Protocol):
    def execute(self, expression: Expression) -> OperationResult: ...


@dataclass(frozen=True, slots=True)
class SyntheticResources:
    """Deployment-placeable logical Resources owned by the Synthetic plugin."""

    specs: ResourceRef = field(
        default_factory=lambda: ResourceRef("structured", "synthetic", "specs")
    )
    run_evidence: ResourceRef = field(
        default_factory=lambda: ResourceRef("structured", "synthetic", "run-evidence")
    )

    def __post_init__(self) -> None:
        selected: list[ResourceRef] = []
        for name in ("specs", "run_evidence"):
            try:
                resource = ResourceRef.parse(getattr(self, name), catalog="structured")
            except (TypeError, ValueError) as exc:
                raise InvalidSpec(
                    f"{name} must be a logical structured Resource",
                    field_path=f"resources.{name}",
                ) from exc
            object.__setattr__(self, name, resource)
            selected.append(resource)
        if len(set(selected)) != len(selected):
            raise InvalidSpec("Synthetic logical Resources must be distinct")


def _single_record(data: object, record_type: str) -> Mapping[str, object] | None:
    if data is None:
        return None
    selected = data
    if isinstance(data, Mapping):
        for key in ("item", "record", "data"):
            if key in data:
                selected = data[key]
                break
    if selected is None:
        return None
    if not isinstance(selected, Mapping):
        raise InvalidSpec(f"Meridian get returned an invalid {record_type} record")
    return MappingProxyType(dict(cast(Mapping[str, object], selected)))


def _mapping(value: object, field_path: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise InvalidSpec(f"{field_path} must be an object", field_path=field_path)
    return cast(Mapping[str, object], value)


class SyntheticRepository:
    """Persist immutable Synthetic resources through public structured Expressions."""

    def __init__(
        self,
        executor: MeridianExecutor,
        resources: SyntheticResources | None = None,
    ) -> None:
        if not callable(getattr(executor, "execute", None)):
            raise TypeError("executor must implement Meridian.execute(Expression)")
        self._executor = executor
        self.resources = resources or SyntheticResources()
        self._surface = StructuredCatalogSurface()

    def _get(
        self,
        resource: ResourceRef,
        where: Mapping[str, object],
        *,
        record_type: str,
    ) -> Mapping[str, object] | None:
        expression = self._surface.get(resource=resource.to_dict(), where=where)
        try:
            result = self._executor.execute(expression)
        except NotFoundError:
            return None
        return _single_record(result.data, record_type)

    def _put(self, resource: ResourceRef, data: Mapping[str, object]) -> OperationResult:
        expression = self._surface.put(
            resource=resource.to_dict(),
            data=data,
            expected_version=0,
        )
        return self._executor.execute(expression)

    def get_spec(self, spec_id: str, version: str) -> SyntheticSpec | None:
        record = self._get(
            self.resources.specs,
            {"specId": spec_id, "specVersion": version},
            record_type="SyntheticSpec",
        )
        if record is None:
            return None
        document = _mapping(record.get("document"), "document")
        spec = SyntheticSpec.load(document)
        expected = {
            "formatVersion": SPEC_RESOURCE_FORMAT,
            "specId": spec.spec_id,
            "specVersion": spec.version,
            "fingerprint": spec.fingerprint,
        }
        if any(record.get(key) != value for key, value in expected.items()):
            raise InvalidSpec("persisted SyntheticSpec resource is incompatible")
        return spec

    def register_spec(self, spec: SyntheticSpec) -> tuple[SyntheticSpec, bool]:
        if not isinstance(spec, SyntheticSpec):
            raise TypeError("spec must be SyntheticSpec")
        existing = self.get_spec(spec.spec_id, spec.version)
        if existing is not None:
            if existing.fingerprint != spec.fingerprint:
                raise SinkConflict(
                    "SyntheticSpec identity already contains different content",
                    details={"specId": spec.spec_id, "specVersion": spec.version},
                )
            return existing, True
        record: Mapping[str, object] = {
            "formatVersion": SPEC_RESOURCE_FORMAT,
            "specId": spec.spec_id,
            "specVersion": spec.version,
            "fingerprint": spec.fingerprint,
            "document": spec.to_dict(),
        }
        try:
            self._put(self.resources.specs, record)
        except ConflictError:
            persisted = self.get_spec(spec.spec_id, spec.version)
            if persisted is None or persisted.fingerprint != spec.fingerprint:
                raise SinkConflict(
                    "SyntheticSpec identity was concurrently changed",
                    details={"specId": spec.spec_id, "specVersion": spec.version},
                ) from None
            return persisted, True
        return spec, False

    def get_run_evidence(self, run_id: str, state: str) -> RunEvidence | None:
        record = self._get(
            self.resources.run_evidence,
            {"runId": run_id, "state": state},
            record_type="RunEvidence",
        )
        if record is None:
            return None
        document = _mapping(record.get("document"), "document")
        evidence = RunEvidence.from_mapping(document)
        fingerprint = sha256_digest(cast(JsonValue, evidence.to_dict()))
        expected = {
            "formatVersion": RUN_EVIDENCE_RESOURCE_FORMAT,
            "runId": evidence.run_id,
            "state": evidence.state,
            "fingerprint": fingerprint,
        }
        if any(record.get(key) != value for key, value in expected.items()):
            raise InvalidSpec("persisted RunEvidence resource is incompatible")
        return evidence

    def record_run_evidence(self, evidence: RunEvidence) -> tuple[RunEvidence, bool]:
        if not isinstance(evidence, RunEvidence):
            raise TypeError("evidence must be RunEvidence")
        existing = self.get_run_evidence(evidence.run_id, evidence.state)
        fingerprint = sha256_digest(cast(JsonValue, evidence.to_dict()))
        if existing is not None:
            existing_fingerprint = sha256_digest(cast(JsonValue, existing.to_dict()))
            if existing_fingerprint != fingerprint:
                raise SinkConflict(
                    "RunEvidence identity already contains different content",
                    details={"runId": evidence.run_id, "state": evidence.state},
                )
            return existing, True
        record: Mapping[str, object] = {
            "formatVersion": RUN_EVIDENCE_RESOURCE_FORMAT,
            "runId": evidence.run_id,
            "state": evidence.state,
            "fingerprint": fingerprint,
            "document": evidence.to_dict(),
        }
        try:
            self._put(self.resources.run_evidence, record)
        except ConflictError:
            persisted = self.get_run_evidence(evidence.run_id, evidence.state)
            if (
                persisted is None
                or sha256_digest(cast(JsonValue, persisted.to_dict())) != fingerprint
            ):
                raise SinkConflict(
                    "RunEvidence identity was concurrently changed",
                    details={"runId": evidence.run_id, "state": evidence.state},
                ) from None
            return persisted, True
        return evidence, False


__all__ = [
    "RUN_EVIDENCE_RESOURCE_FORMAT",
    "SPEC_RESOURCE_FORMAT",
    "SyntheticRepository",
    "SyntheticResources",
]
