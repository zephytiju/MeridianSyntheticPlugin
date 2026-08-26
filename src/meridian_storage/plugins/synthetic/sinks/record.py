# SPDX-License-Identifier: Apache-2.0
"""Direct Record writes through the public Meridian ``structured`` Catalog."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol, cast

from meridian_storage import Expression, Meridian, OperationContext, OperationResult
from meridian_storage.semantics import RecordReference, SchemaDocument

from ..canonical import JsonValue
from ..errors import SinkConflict
from .base import (
    CheckpointStore,
    InMemoryCheckpointStore,
    PartitionReceipt,
    RecordPartition,
    SinkKind,
)


class TargetClass(StrEnum):
    DISPOSABLE = "disposable"
    STAGING = "staging"
    PRODUCTION = "production"


@dataclass(frozen=True, slots=True)
class DirectWritePolicy:
    target_class: TargetClass = TargetClass.DISPOSABLE
    resume: bool = True
    cleanup_on_failure: bool = False
    production_approved: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "target_class", TargetClass(self.target_class))
        if not all(
            isinstance(value, bool)
            for value in (self.resume, self.cleanup_on_failure, self.production_approved)
        ):
            raise TypeError("direct write policy flags must be booleans")
        if self.target_class is TargetClass.PRODUCTION and not self.production_approved:
            raise ValueError("production target writes require explicit owning-process approval")
        if self.cleanup_on_failure and self.target_class is not TargetClass.STAGING:
            raise ValueError("cleanup_on_failure is valid only for staging targets")


class _StructuredSurface(Protocol):
    def put(
        self,
        *,
        resource: str | Mapping[str, object],
        data: Mapping[str, object],
        expected_version: str | int | None = None,
    ) -> Expression: ...

    def delete(
        self,
        *,
        resource: str | Mapping[str, object],
        where: Mapping[str, object],
        expected_version: str | int | None = None,
    ) -> Expression: ...


class _Runtime(Protocol):
    def catalog(self, name: str) -> object: ...

    def context(self, context: OperationContext) -> AbstractContextManager[OperationContext]: ...

    def execute(self, expression: Expression) -> OperationResult: ...


class MeridianRecordSink:
    """Partition-resumable writes without Adapter or physical-storage access."""

    kind = SinkKind.RECORDS

    def __init__(
        self,
        meridian: Meridian,
        context: OperationContext,
        *,
        policy: DirectWritePolicy,
        checkpoint_store: CheckpointStore | None = None,
    ) -> None:
        self._meridian = cast(_Runtime, meridian)
        self._context = context
        self._policy = policy
        self._checkpoints = checkpoint_store or InMemoryCheckpointStore()
        self._written: dict[str, list[RecordReference]] = {}

    def completed_partitions(self, run_id: str) -> frozenset[str]:
        return self._checkpoints.completed(run_id) if self._policy.resume else frozenset()

    def write_partition(self, run_id: str, partition: RecordPartition) -> PartitionReceipt:
        receipt = PartitionReceipt.from_partition(partition)
        completed = self._checkpoints.completed(run_id) if self._policy.resume else frozenset()
        if partition.partition_id in completed:
            return PartitionReceipt.from_partition(partition, skipped=True)
        try:
            surface = cast(_StructuredSurface, self._meridian.catalog("structured"))
            for offset, record in enumerate(partition.records):
                context = replace(
                    self._context,
                    idempotency_key=f"{run_id}:{partition.partition_id}:{offset}",
                )
                expression = surface.put(
                    resource=record.collection_ref.to_dict(), data=record.to_dict()
                )
                with self._meridian.context(context):
                    self._meridian.execute(expression)
                if self._policy.cleanup_on_failure:
                    self._written.setdefault(run_id, []).append(
                        RecordReference(record.collection_ref, record.record_id)
                    )
            self._checkpoints.mark_completed(run_id, receipt)
            return receipt
        except SinkConflict:
            raise
        except Exception as exc:
            raise SinkConflict(
                "Meridian structured write failed",
                checkpoint=tuple(sorted(completed)),
                details={"partitionId": partition.partition_id},
            ) from exc

    def finalize(
        self,
        run_id: str,
        receipts: Sequence[PartitionReceipt],
        schemas: Mapping[str, SchemaDocument],
    ) -> Mapping[str, JsonValue]:
        del schemas
        result = MappingProxyType(
            {
                "kind": self.kind.value,
                "runId": run_id,
                "partitions": len(receipts),
                "records": sum(item.record_count for item in receipts),
            }
        )
        # Successful finalization makes staging cleanup unnecessary and releases
        # the bounded set of public record references retained for abort handling.
        self._written.pop(run_id, None)
        return result

    def abort(self, run_id: str, receipts: Sequence[PartitionReceipt], reason: str) -> None:
        del receipts, reason
        if not self._policy.cleanup_on_failure:
            return
        surface = cast(_StructuredSurface, self._meridian.catalog("structured"))
        for offset, record in enumerate(reversed(self._written.get(run_id, []))):
            context = replace(self._context, idempotency_key=f"{run_id}:cleanup:{offset}")
            expression = surface.delete(
                resource=record.collection_ref.to_dict(),
                where={"recordId": record.record_id},
            )
            try:
                with self._meridian.context(context):
                    self._meridian.execute(expression)
            except Exception:
                # Cleanup is best-effort and must not hide the original bounded failure.
                continue


__all__ = ["DirectWritePolicy", "MeridianRecordSink", "TargetClass"]
