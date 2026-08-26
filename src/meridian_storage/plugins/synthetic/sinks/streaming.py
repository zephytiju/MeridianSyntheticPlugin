# SPDX-License-Identifier: Apache-2.0
"""Provider-neutral Event publication through Meridian Streaming V1."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import replace
from types import MappingProxyType
from typing import Protocol, cast

from meridian_storage import (
    Expression,
    Meridian,
    OperationContext,
    OperationResult,
    ResourceRef,
    SchemaRef,
)
from meridian_storage.semantics import SchemaDocument
from meridian_storage.streaming import MAX_BATCH_SIZE, Event

from ..canonical import JsonValue, canonical_json_bytes, sha256_digest
from ..errors import SinkConflict
from .base import (
    CheckpointStore,
    InMemoryCheckpointStore,
    PartitionReceipt,
    RecordPartition,
    SinkKind,
)


class _StreamingSurface(Protocol):
    def publish_batch(
        self,
        *,
        resource: str | Mapping[str, object],
        data: Sequence[Mapping[str, object]],
        idempotency_key: str | None = None,
    ) -> Expression: ...


class _Runtime(Protocol):
    def catalog(self, name: str) -> object: ...

    def context(self, context: OperationContext) -> AbstractContextManager[OperationContext]: ...

    def execute(self, expression: Expression) -> OperationResult: ...


class MeridianStreamingSink:
    kind = SinkKind.STREAMING

    def __init__(
        self,
        meridian: Meridian,
        context: OperationContext,
        *,
        streams: Mapping[str, ResourceRef | str | Mapping[str, object]],
        schemas: Mapping[str, SchemaRef | Mapping[str, object]],
        checkpoint_store: CheckpointStore | None = None,
    ) -> None:
        if set(streams) != set(schemas) or not streams:
            raise ValueError("stream and schema mappings must be non-empty with identical keys")
        self._meridian = cast(_Runtime, meridian)
        self._context = context
        self._streams = MappingProxyType(
            {name: ResourceRef.parse(value, catalog="streaming") for name, value in streams.items()}
        )
        self._schemas = MappingProxyType(
            {name: SchemaRef.parse(value) for name, value in schemas.items()}
        )
        for name, schema in self._schemas.items():
            if schema.catalog != "streaming" or schema.namespace != self._streams[name].namespace:
                raise ValueError("streaming Schema must share its Stream Namespace")
        self._checkpoints = checkpoint_store or InMemoryCheckpointStore()

    def completed_partitions(self, run_id: str) -> frozenset[str]:
        return self._checkpoints.completed(run_id)

    def _event(self, run_id: str, partition: RecordPartition, index: int) -> Event:
        record = partition.records[index]
        stream = self._streams[partition.collection_id]
        schema = self._schemas[partition.collection_id]
        identity = sha256_digest(
            canonical_json_bytes(
                cast(JsonValue, [run_id, partition.partition_id, index, record.record_id])
            )
        ).removeprefix("sha256:")
        partition_key = sha256_digest(cast(JsonValue, record.record_id)).removeprefix("sha256:")
        return Event(
            event_id=f"synthetic-{identity}",
            stream=stream,
            schema=schema,
            data=cast(Mapping[str, object], record.to_dict()),  # type: ignore[arg-type]
            occurred_at=cast(str, record.updated_at),
            produced_at=cast(str, record.updated_at),
            logical_partition_key=partition_key,
            headers={
                "meridian.synthetic.run": run_id,
                "meridian.synthetic.partition": partition.partition_id,
            },
            extensions={"org.meridian.synthetic/sourceCollection": record.collection_ref.to_dict()},
        )

    def write_partition(self, run_id: str, partition: RecordPartition) -> PartitionReceipt:
        receipt = PartitionReceipt.from_partition(partition)
        completed = self._checkpoints.completed(run_id)
        if partition.partition_id in completed:
            return PartitionReceipt.from_partition(partition, skipped=True)
        if partition.collection_id not in self._streams:
            raise SinkConflict(
                "streaming sink has no Stream mapping for Collection",
                details={"collectionId": partition.collection_id},
            )
        try:
            surface = cast(_StreamingSurface, self._meridian.catalog("streaming"))
            events = [
                self._event(run_id, partition, index) for index in range(len(partition.records))
            ]
            for batch_index, offset in enumerate(range(0, len(events), MAX_BATCH_SIZE)):
                selected = events[offset : offset + MAX_BATCH_SIZE]
                idempotency_key = f"{run_id}:{partition.partition_id}:{batch_index}"
                expression = surface.publish_batch(
                    resource=self._streams[partition.collection_id].to_dict(),
                    data=[item.to_dict() for item in selected],
                    idempotency_key=idempotency_key,
                )
                with self._meridian.context(
                    replace(self._context, idempotency_key=idempotency_key)
                ):
                    self._meridian.execute(expression)
            self._checkpoints.mark_completed(run_id, receipt)
            return receipt
        except SinkConflict:
            raise
        except Exception as exc:
            raise SinkConflict(
                "Meridian streaming publish_batch failed",
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
        return MappingProxyType(
            {
                "kind": self.kind.value,
                "runId": run_id,
                "partitions": len(receipts),
                "events": sum(item.record_count for item in receipts),
            }
        )

    def abort(self, run_id: str, receipts: Sequence[PartitionReceipt], reason: str) -> None:
        del run_id, receipts, reason


__all__ = ["MeridianStreamingSink"]
