# SPDX-License-Identifier: Apache-2.0
"""Bounded sink contracts, partitions, receipts, and checkpoints."""

from __future__ import annotations

import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol, cast, runtime_checkable

from meridian_storage.semantics import Record, SchemaDocument

from ..canonical import JsonValue, canonical_json_bytes, require_digest, sha256_digest


class SinkKind(StrEnum):
    MEMORY = "memory"
    RECORDS = "records"
    STREAMING = "streaming"
    DATASET = "dataset"


def record_bytes(record: Record) -> bytes:
    return canonical_json_bytes(cast(JsonValue, record.to_dict()))


@dataclass(frozen=True, slots=True)
class RecordPartition:
    collection_id: str
    partition_id: str
    start_ordinal: int
    records: tuple[Record, ...]

    def __post_init__(self) -> None:
        if not self.collection_id or not self.partition_id or self.start_ordinal < 0:
            raise ValueError("partition identity is invalid")
        if not self.records:
            raise ValueError("partition must contain at least one Record")

    @property
    def record_count(self) -> int:
        return len(self.records)

    @property
    def payload(self) -> bytes:
        return b"".join(record_bytes(item) + b"\n" for item in self.records)


@dataclass(frozen=True, slots=True)
class PartitionReceipt:
    collection_id: str
    partition_id: str
    record_count: int
    byte_count: int
    digest: str
    skipped: bool = False
    provenance: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        if not self.collection_id or not self.partition_id:
            raise ValueError("partition receipt identity must be non-empty")
        if self.record_count < 1 or self.byte_count < 1:
            raise ValueError("partition receipt counts must be positive")
        object.__setattr__(self, "digest", require_digest(self.digest, "partition digest"))
        if not isinstance(self.skipped, bool):
            raise TypeError("partition receipt skipped must be boolean")
        if any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in self.provenance.items()
        ):
            raise TypeError("partition receipt provenance must contain strings")
        object.__setattr__(
            self,
            "provenance",
            MappingProxyType(dict(sorted(self.provenance.items()))),
        )

    @classmethod
    def from_partition(
        cls,
        partition: RecordPartition,
        *,
        skipped: bool = False,
        provenance: Mapping[str, str] | None = None,
    ) -> PartitionReceipt:
        payload = partition.payload
        return cls(
            collection_id=partition.collection_id,
            partition_id=partition.partition_id,
            record_count=partition.record_count,
            byte_count=len(payload),
            digest=sha256_digest(payload),
            skipped=skipped,
            provenance=MappingProxyType(dict(sorted((provenance or {}).items()))),
        )

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "collectionId": self.collection_id,
            "partitionId": self.partition_id,
            "recordCount": self.record_count,
            "byteCount": self.byte_count,
            "digest": self.digest,
            "skipped": self.skipped,
            "provenance": dict(self.provenance),
        }


@runtime_checkable
class CheckpointStore(Protocol):
    def completed(self, run_id: str) -> frozenset[str]: ...

    def mark_completed(self, run_id: str, receipt: PartitionReceipt) -> None: ...


class InMemoryCheckpointStore:
    """Thread-safe reference checkpoint store for tests and disposable runs."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._receipts: dict[str, dict[str, PartitionReceipt]] = {}

    def completed(self, run_id: str) -> frozenset[str]:
        with self._lock:
            return frozenset(self._receipts.get(run_id, {}))

    def mark_completed(self, run_id: str, receipt: PartitionReceipt) -> None:
        with self._lock:
            existing = self._receipts.setdefault(run_id, {}).get(receipt.partition_id)
            if existing is not None and existing.digest != receipt.digest:
                raise ValueError("checkpoint partition digest conflict")
            self._receipts[run_id][receipt.partition_id] = receipt


@runtime_checkable
class RecordSink(Protocol):
    @property
    def kind(self) -> SinkKind: ...

    def completed_partitions(self, run_id: str) -> frozenset[str]: ...

    def write_partition(self, run_id: str, partition: RecordPartition) -> PartitionReceipt: ...

    def finalize(
        self,
        run_id: str,
        receipts: Sequence[PartitionReceipt],
        schemas: Mapping[str, SchemaDocument],
    ) -> Mapping[str, JsonValue]: ...

    def abort(self, run_id: str, receipts: Sequence[PartitionReceipt], reason: str) -> None: ...


class MemorySink:
    """Bounded test sink; callers opt into retaining Records in process memory."""

    kind = SinkKind.MEMORY

    def __init__(
        self,
        *,
        checkpoint_store: CheckpointStore | None = None,
        max_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        self._checkpoints = checkpoint_store or InMemoryCheckpointStore()
        self._max_bytes = max_bytes
        self._partitions: dict[str, dict[str, RecordPartition]] = {}
        self._bytes: dict[str, int] = {}

    def completed_partitions(self, run_id: str) -> frozenset[str]:
        return self._checkpoints.completed(run_id)

    def write_partition(self, run_id: str, partition: RecordPartition) -> PartitionReceipt:
        receipt = PartitionReceipt.from_partition(partition)
        existing = self._partitions.setdefault(run_id, {}).get(partition.partition_id)
        if existing is not None:
            previous = PartitionReceipt.from_partition(existing)
            if previous.digest != receipt.digest:
                raise ValueError("MemorySink partition digest conflict")
            return PartitionReceipt.from_partition(existing, skipped=True)
        total = self._bytes.get(run_id, 0) + receipt.byte_count
        if total > self._max_bytes:
            raise MemoryError("MemorySink byte limit exceeded")
        self._partitions[run_id][partition.partition_id] = partition
        self._bytes[run_id] = total
        self._checkpoints.mark_completed(run_id, receipt)
        return receipt

    def records(self, run_id: str, collection_id: str | None = None) -> tuple[Record, ...]:
        partitions = self._partitions.get(run_id, {})
        selected = sorted(partitions.values(), key=lambda item: item.partition_id)
        return tuple(
            record
            for partition in selected
            if collection_id is None or partition.collection_id == collection_id
            for record in partition.records
        )

    def finalize(
        self,
        run_id: str,
        receipts: Sequence[PartitionReceipt],
        schemas: Mapping[str, SchemaDocument],
    ) -> Mapping[str, JsonValue]:
        del schemas
        return MappingProxyType(
            {
                "kind": self.kind.value,
                "runId": run_id,
                "partitions": len(receipts),
                "bytes": sum(item.byte_count for item in receipts),
            }
        )

    def abort(self, run_id: str, receipts: Sequence[PartitionReceipt], reason: str) -> None:
        del run_id, receipts, reason


__all__ = [
    "CheckpointStore",
    "InMemoryCheckpointStore",
    "MemorySink",
    "PartitionReceipt",
    "RecordPartition",
    "RecordSink",
    "SinkKind",
    "record_bytes",
]
