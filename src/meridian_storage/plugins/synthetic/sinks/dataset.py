# SPDX-License-Identifier: Apache-2.0
"""Canonical dataset partitions retained for indirect artifact publication."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Protocol, runtime_checkable

from meridian_storage.semantics import SchemaDocument

from ..canonical import JsonValue, sha256_digest
from ..errors import ExceededBudget, SinkConflict
from .base import (
    CheckpointStore,
    InMemoryCheckpointStore,
    PartitionReceipt,
    RecordPartition,
    SinkKind,
)


@runtime_checkable
class PartitionStore(Protocol):
    def put(self, run_id: str, partition_id: str, payload: bytes, digest: str) -> None: ...

    def get(self, run_id: str, partition_id: str) -> bytes: ...


class InMemoryPartitionStore:
    def __init__(self, *, max_bytes: int) -> None:
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        self._max_bytes = max_bytes
        self._values: dict[tuple[str, str], tuple[bytes, str]] = {}
        self._totals: dict[str, int] = {}

    def put(self, run_id: str, partition_id: str, payload: bytes, digest: str) -> None:
        key = (run_id, partition_id)
        existing = self._values.get(key)
        if existing is not None:
            if existing[1] != digest or existing[0] != payload:
                raise SinkConflict("dataset partition digest conflict")
            return
        total = self._totals.get(run_id, 0) + len(payload)
        if total > self._max_bytes:
            raise ExceededBudget("dataset PartitionStore memory bound exceeded")
        self._values[key] = (bytes(payload), digest)
        self._totals[run_id] = total

    def get(self, run_id: str, partition_id: str) -> bytes:
        try:
            return self._values[(run_id, partition_id)][0]
        except KeyError as exc:
            raise KeyError(f"unknown dataset partition {partition_id!r}") from exc


class CanonicalDatasetSink:
    """Encode JSON Lines partitions; publication remains an explicit later action."""

    kind = SinkKind.DATASET

    def __init__(
        self,
        *,
        store: PartitionStore,
        checkpoint_store: CheckpointStore | None = None,
    ) -> None:
        self._store = store
        self._checkpoints = checkpoint_store or InMemoryCheckpointStore()

    def completed_partitions(self, run_id: str) -> frozenset[str]:
        return self._checkpoints.completed(run_id)

    def write_partition(self, run_id: str, partition: RecordPartition) -> PartitionReceipt:
        receipt = PartitionReceipt.from_partition(partition)
        if partition.partition_id in self._checkpoints.completed(run_id):
            existing = self._store.get(run_id, partition.partition_id)
            if len(existing) != receipt.byte_count or sha256_digest(existing) != receipt.digest:
                raise SinkConflict("dataset checkpoint payload conflict")
            return PartitionReceipt.from_partition(partition, skipped=True)
        self._store.put(run_id, partition.partition_id, partition.payload, receipt.digest)
        self._checkpoints.mark_completed(run_id, receipt)
        return receipt

    def partition_bytes(self, run_id: str, partition_id: str) -> bytes:
        return self._store.get(run_id, partition_id)

    def finalize(
        self,
        run_id: str,
        receipts: Sequence[PartitionReceipt],
        schemas: Mapping[str, SchemaDocument],
    ) -> Mapping[str, JsonValue]:
        return MappingProxyType(
            {
                "kind": self.kind.value,
                "runId": run_id,
                "mediaType": "application/x-ndjson",
                "partitions": len(receipts),
                "schemas": len(schemas),
                "bytes": sum(item.byte_count for item in receipts),
            }
        )

    def abort(self, run_id: str, receipts: Sequence[PartitionReceipt], reason: str) -> None:
        del run_id, receipts, reason


__all__ = ["CanonicalDatasetSink", "InMemoryPartitionStore", "PartitionStore"]
