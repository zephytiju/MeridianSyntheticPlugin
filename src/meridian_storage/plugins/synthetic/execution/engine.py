# SPDX-License-Identifier: Apache-2.0
"""Deterministic bounded execution for SyntheticSpec V1."""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from enum import StrEnum
from types import MappingProxyType
from typing import cast

from meridian_storage.semantics import (
    Cardinality,
    FieldDefinition,
    FrozenJson,
    Record,
    RecordReference,
    validate_record,
)

from ..canonical import (
    JsonValue,
    canonical_json_bytes,
    derive_seed,
    deterministic_uuid,
    sha256_digest,
)
from ..errors import (
    CancelledRun,
    ExceededBudget,
    InvalidSpec,
    PublicationFailure,
    SinkConflict,
    SyntheticError,
    ValidationFailure,
)
from ..evidence import CollectingEvidenceHook, EvidenceHook, RunEvidence
from ..generators import (
    GenerationContext,
    GeneratorRegistry,
    builtin_registry,
    default_generator_id,
)
from ..publication import ArtifactPublication, ArtifactPublisher, DatasetManifest
from ..sinks import (
    CanonicalDatasetSink,
    MemorySink,
    PartitionReceipt,
    RecordPartition,
    RecordSink,
    SinkKind,
)
from ..spec import (
    CollectionSpec,
    CorrelationSpec,
    FieldGeneratorSpec,
    OutputMode,
    RelationSpec,
    SyntheticSpec,
)
from ..validation import ValidationAccumulator, ValidationReport
from .sources import (
    SourceReader,
    SourceSnapshot,
    SourceTransformationRegistry,
    builtin_source_transformations,
    materialize_sources,
)

IMPLEMENTATION_COORDINATE = "meridian-storage-plugin-synthetic@1.0.1"


class RunState(StrEnum):
    PLANNED = "PLANNED"
    RUNNING = "RUNNING"
    VALIDATING = "VALIDATING"
    PUBLISHED = "PUBLISHED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class CancellationToken:
    def __init__(self) -> None:
        self._event = threading.Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()


@dataclass(frozen=True, slots=True)
class RunProgress:
    generated_records: int
    written_partitions: int
    generated_bytes: int

    def to_dict(self) -> dict[str, int]:
        return {
            "generatedRecords": self.generated_records,
            "writtenPartitions": self.written_partitions,
            "generatedBytes": self.generated_bytes,
        }


class SyntheticRun:
    """Completed run, deterministic evidence, and optional dataset publication action."""

    def __init__(
        self,
        *,
        run_id: str,
        spec: SyntheticSpec,
        state: RunState,
        implementation_digest: str,
        receipts: Sequence[PartitionReceipt],
        validation: ValidationReport,
        sink_result: Mapping[str, JsonValue],
        source_evidence: Sequence[Mapping[str, JsonValue]],
        evidence: Sequence[RunEvidence],
        evidence_hook: EvidenceHook | None,
        sink: RecordSink,
        progress: RunProgress,
    ) -> None:
        self.run_id = run_id
        self.spec = spec
        self.state = state
        self.implementation_digest = implementation_digest
        self.receipts = tuple(receipts)
        self.validation = validation
        self.sink_result = MappingProxyType(dict(sink_result))
        self.source_evidence = tuple(source_evidence)
        self.evidence = tuple(evidence)
        self.progress = progress
        self._sink = sink
        self._evidence_hook = evidence_hook
        self.publication: ArtifactPublication | None = None

    @property
    def manifest(self) -> DatasetManifest:
        if not isinstance(self._sink, CanonicalDatasetSink):
            raise PublicationFailure("only dataset output has an artifact manifest")
        return DatasetManifest(
            run_id=self.run_id,
            spec_id=self.spec.spec_id,
            spec_version=self.spec.version,
            spec_fingerprint=self.spec.fingerprint,
            implementation_coordinate=IMPLEMENTATION_COORDINATE,
            implementation_digest=self.implementation_digest,
            schema_fingerprints=self.spec.schema_fingerprints,
            partitions=self.receipts,
            validation_digest=self.validation.digest,
            source_evidence=self.source_evidence,
        )

    def publish(self, publisher: ArtifactPublisher) -> ArtifactPublication:
        """Hand a canonical dataset to the Configuration & Artifact plugin seam."""

        if self.publication is not None:
            return self.publication
        if self.state is not RunState.PUBLISHED:
            raise PublicationFailure("only a successful run can publish an artifact")
        if not isinstance(self._sink, CanonicalDatasetSink):
            raise PublicationFailure("artifact publication requires dataset output")
        manifest = self.manifest
        partitions = {
            receipt.partition_id: self._sink.partition_bytes(self.run_id, receipt.partition_id)
            for receipt in self.receipts
        }
        try:
            publication = publisher.publish(manifest, partitions)
        except PublicationFailure:
            raise
        except Exception as exc:
            raise PublicationFailure("artifact publisher rejected the dataset") from exc
        if publication.digest != manifest.artifact_digest:
            raise PublicationFailure("artifact publisher returned an unexpected digest")
        artifact_evidence = RunEvidence(
            run_id=self.run_id,
            state=RunState.PUBLISHED.value,
            spec_fingerprint=self.spec.fingerprint,
            implementation_digest=self.implementation_digest,
            schema_fingerprints=self.spec.schema_fingerprints,
            partition_digests={item.partition_id: item.digest for item in self.receipts},
            query_fingerprints=tuple(
                cast(str, item["queryFingerprint"])
                for item in self.source_evidence
                if "queryFingerprint" in item
            ),
            source_evidence=self.source_evidence,
            validation_digest=self.validation.digest,
            artifact_digest=publication.digest,
        )
        if self._evidence_hook is not None:
            self._evidence_hook.emit(artifact_evidence)
        self.evidence = (*self.evidence, artifact_evidence)
        self.publication = publication
        return publication


class Generator:
    """Compile and execute one immutable SyntheticSpec against an explicit sink."""

    def __init__(
        self,
        spec: SyntheticSpec | bytes | str | Mapping[str, object],
        *,
        registry: GeneratorRegistry | None = None,
        source_transformations: SourceTransformationRegistry | None = None,
    ) -> None:
        self.spec = spec if isinstance(spec, SyntheticSpec) else SyntheticSpec.load(spec)
        self.registry = (registry or builtin_registry()).seal()
        self.source_transformations = (
            source_transformations or builtin_source_transformations()
        ).seal()
        self.implementation_digest = sha256_digest(
            cast(
                JsonValue,
                {
                    "coordinate": IMPLEMENTATION_COORDINATE,
                    "generators": self.registry.implementation_digest,
                    "sourceTransformations": self.source_transformations.implementation_digest,
                },
            )
        )
        accepted_coordinates = {IMPLEMENTATION_COORDINATE}
        if self.spec.implementation.coordinate not in accepted_coordinates:
            raise InvalidSpec("SyntheticSpec implementation coordinate targets another package")
        required = self.spec.implementation.required_digest
        if required is not None and required != self.implementation_digest:
            raise InvalidSpec("SyntheticSpec implementation digest pin does not match")
        self._preflight_generators()

    def _preflight_generators(self) -> None:
        for collection in self._all_collections():
            for field in collection.schema.fields:
                field_spec = collection.fields.get(
                    field.name,
                    FieldGeneratorSpec(default_generator_id(field.logical_type.kind)),
                )
                self.registry.resolve(field_spec, field, self.spec.locales)

    def run(
        self,
        sink: RecordSink | None = None,
        *,
        run_id: str | None = None,
        source_reader: SourceReader | None = None,
        evidence_hook: EvidenceHook | None = None,
        cancellation: CancellationToken | None = None,
        workers: int = 1,
    ) -> SyntheticRun:
        if isinstance(workers, bool) or not isinstance(workers, int) or not 1 <= workers <= 64:
            raise ValueError("workers must be between 1 and 64")
        selected_sink = sink or MemorySink(max_bytes=self.spec.bounds.max_memory_bytes)
        self._validate_sink(selected_sink)
        selected_run_id = run_id or self._default_run_id()
        if not selected_run_id or len(selected_run_id.encode()) > 256:
            raise ValueError("run_id must be non-empty and bounded")
        cancellation = cancellation or CancellationToken()
        local_evidence = CollectingEvidenceHook()
        started = time.monotonic()
        receipts: list[PartitionReceipt] = []
        progress = RunProgress(0, 0, 0)
        source_snapshots: Mapping[str, SourceSnapshot] = MappingProxyType({})

        def emit(state: RunState, error_code: str | None = None) -> None:
            item = RunEvidence(
                run_id=selected_run_id,
                state=state.value,
                spec_fingerprint=self.spec.fingerprint,
                implementation_digest=self.implementation_digest,
                schema_fingerprints=self.spec.schema_fingerprints,
                partition_digests={item.partition_id: item.digest for item in receipts},
                query_fingerprints=tuple(item.query_fingerprint for item in self.spec.sources),
                source_evidence=tuple(
                    snapshot.evidence() for snapshot in source_snapshots.values()
                ),
                error_code=error_code,
            )
            local_evidence.emit(item)
            if evidence_hook is not None:
                evidence_hook.emit(item)

        emit(RunState.PLANNED)
        try:
            source_snapshots = materialize_sources(
                self.spec.sources,
                reader=source_reader,
                transformations=self.source_transformations,
                max_memory_bytes=self.spec.bounds.max_memory_bytes,
            )
            retained_memory = sum(snapshot.memory_bytes for snapshot in source_snapshots.values())

            def reserve_memory(amount: int) -> None:
                nonlocal retained_memory
                retained_memory += amount
                if retained_memory > self.spec.bounds.max_memory_bytes:
                    raise ExceededBudget("retained run state exceeds maxMemoryBytes")

            emit(RunState.RUNNING)
            validation = ValidationAccumulator(
                expected_counts={
                    item.collection_id: item.count for item in self._all_collections()
                },
                identity_fields={
                    item.collection_id: item.schema.identity for item in self._all_collections()
                },
                rules=self.spec.validation_rules,
                reserve_memory=reserve_memory,
            )
            references: dict[str, list[Mapping[str, JsonValue]]] = defaultdict(list)
            reference_ids: dict[str, set[bytes]] = defaultdict(set)
            collections = {item.collection_id: item for item in self._all_collections()}
            relations = {item.collection.collection_id: item for item in self.spec.relations}

            for collection_id in self._execution_order():
                self._check_control(cancellation, started)
                collection = collections[collection_id]
                relation = relations.get(collection_id)
                for partition_index, start in enumerate(
                    range(0, collection.count, self.spec.bounds.batch_size)
                ):
                    self._check_control(cancellation, started)
                    stop = min(collection.count, start + self.spec.bounds.batch_size)
                    ordinals = tuple(range(start, stop))
                    records = self._generate_records(
                        collection,
                        ordinals,
                        selected_run_id,
                        source_snapshots,
                        references,
                        relation,
                        workers,
                    )
                    self._check_control(cancellation, started)
                    partition = RecordPartition(
                        collection_id=collection_id,
                        partition_id=f"{collection_id}-{partition_index:012d}",
                        start_ordinal=start,
                        records=records,
                    )
                    expected_receipt = PartitionReceipt.from_partition(partition)
                    generated_bytes = progress.generated_bytes + expected_receipt.byte_count
                    if expected_receipt.byte_count > self.spec.bounds.max_memory_bytes:
                        raise ExceededBudget("generated partition exceeds maxMemoryBytes")
                    if generated_bytes > self.spec.bounds.max_bytes:
                        raise ExceededBudget("generated data exceeds bounds.maxBytes")
                    receipt = selected_sink.write_partition(selected_run_id, partition)
                    if (
                        receipt.collection_id != expected_receipt.collection_id
                        or receipt.partition_id != expected_receipt.partition_id
                        or receipt.record_count != expected_receipt.record_count
                        or receipt.byte_count != expected_receipt.byte_count
                        or receipt.digest != expected_receipt.digest
                    ):
                        raise SinkConflict(
                            "sink returned a receipt that differs from its partition"
                        )
                    receipts.append(receipt)
                    generated_records = progress.generated_records + len(records)
                    progress = RunProgress(
                        generated_records,
                        progress.written_partitions + 1,
                        generated_bytes,
                    )
                    for record in records:
                        validation.observe(collection_id, record)
                        reference = RecordReference(
                            record.collection_ref, record.record_id
                        ).to_dict()
                        reserve_memory(len(canonical_json_bytes(cast(JsonValue, reference))) + 96)
                        references[collection_id].append(reference)
                        references[record.collection_ref.canonical].append(reference)
                        reference_ids[record.collection_ref.canonical].add(
                            canonical_json_bytes(cast(JsonValue, record.record_id))
                        )
            emit(RunState.VALIDATING)
            report = validation.finish(
                reference_identities={
                    name: frozenset(values) for name, values in reference_ids.items()
                }
            )
            if not report.passed:
                raise ValidationFailure(
                    "blocking validation rules failed",
                    details={"validationDigest": report.digest},
                )
            sink_result = selected_sink.finalize(
                selected_run_id,
                receipts,
                {item.collection_id: item.schema for item in self._all_collections()},
            )
            final_evidence = RunEvidence(
                run_id=selected_run_id,
                state=RunState.PUBLISHED.value,
                spec_fingerprint=self.spec.fingerprint,
                implementation_digest=self.implementation_digest,
                schema_fingerprints=self.spec.schema_fingerprints,
                partition_digests={item.partition_id: item.digest for item in receipts},
                query_fingerprints=tuple(item.query_fingerprint for item in self.spec.sources),
                source_evidence=tuple(
                    snapshot.evidence() for snapshot in source_snapshots.values()
                ),
                validation_digest=report.digest,
            )
            local_evidence.emit(final_evidence)
            if evidence_hook is not None:
                evidence_hook.emit(final_evidence)
            return SyntheticRun(
                run_id=selected_run_id,
                spec=self.spec,
                state=RunState.PUBLISHED,
                implementation_digest=self.implementation_digest,
                receipts=receipts,
                validation=report,
                sink_result=sink_result,
                source_evidence=tuple(
                    snapshot.evidence() for snapshot in source_snapshots.values()
                ),
                evidence=local_evidence.events,
                evidence_hook=evidence_hook,
                sink=selected_sink,
                progress=progress,
            )
        except Exception as exc:
            state = RunState.CANCELLED if isinstance(exc, CancelledRun) else RunState.FAILED
            try:
                selected_sink.abort(selected_run_id, receipts, type(exc).__name__)
            finally:
                code = exc.code.value if isinstance(exc, SyntheticError) else type(exc).__name__
                emit(state, code)
            raise

    def _generate_records(
        self,
        collection: CollectionSpec,
        ordinals: Sequence[int],
        run_id: str,
        sources: Mapping[str, SourceSnapshot],
        references: Mapping[str, Sequence[Mapping[str, JsonValue]]],
        relation: RelationSpec | None,
        workers: int,
    ) -> tuple[Record, ...]:
        def generate(ordinal: int) -> Record:
            overrides: dict[str, object] = {}
            if relation is not None:
                source_pool = references.get(relation.source_collection, ())
                target_pool = references.get(relation.target_collection, ())
                if not source_pool or not target_pool:
                    raise InvalidSpec("Relation endpoints were not generated before edges")
                overrides[relation.source_field] = source_pool[ordinal % len(source_pool)]
                target_index = derive_seed(
                    self.spec.seed, relation.collection.collection_id, ordinal, "target"
                ) % len(target_pool)
                overrides[relation.target_field] = target_pool[target_index]
            return self._generate_record(
                collection,
                ordinal,
                run_id,
                sources,
                references,
                overrides,
            )

        if workers == 1:
            return tuple(generate(item) for item in ordinals)
        with ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="meridian-synthetic"
        ) as pool:
            return tuple(pool.map(generate, ordinals))

    def _generate_record(
        self,
        collection: CollectionSpec,
        ordinal: int,
        run_id: str,
        sources: Mapping[str, SourceSnapshot],
        references: Mapping[str, Sequence[Mapping[str, JsonValue]]],
        overrides: Mapping[str, object],
    ) -> Record:
        values: dict[str, object] = {}
        source_row: Mapping[str, FrozenJson] = MappingProxyType({})
        if collection.source_id is not None:
            snapshot = sources[collection.source_id]
            source_row = snapshot.rows[ordinal % snapshot.row_count]
        locale = self.spec.locales[ordinal % len(self.spec.locales)]
        for field in collection.schema.fields:
            if field.name in overrides:
                values[field.name] = overrides[field.name]
                continue
            field_spec = collection.fields.get(
                field.name,
                FieldGeneratorSpec(default_generator_id(field.logical_type.kind)),
            )
            definition = self.registry.resolve(field_spec, field, self.spec.locales)
            context = GenerationContext(
                spec_seed=self.spec.seed,
                run_id=run_id,
                collection_id=collection.collection_id,
                field=field,
                ordinal=ordinal,
                element_index=0,
                locale=locale,
                time_start=self.spec.time_bounds.start,
                time_end=self.spec.time_bounds.end,
                values=cast(Mapping[str, FrozenJson], values),
                source=source_row,
                references=references,
                identity_field=field.name in collection.schema.identity,
            )
            if self._is_null(context, field_spec, field):
                values[field.name] = None
            elif field.cardinality is Cardinality.MANY:
                values[field.name] = [
                    definition.generate(replace(context, element_index=index), field_spec.config)
                    for index in range(self._many_count(context, field))
                ]
            else:
                values[field.name] = definition.generate(context, field_spec.config)
        for correlation in collection.correlations:
            values[correlation.field] = self._correlate(correlation, values)
        normalized = validate_record(collection.schema, values)
        identity_values = tuple(normalized[name] for name in collection.schema.identity)
        record_id: FrozenJson = identity_values[0] if len(identity_values) == 1 else identity_values
        timestamp = self.spec.time_bounds.start
        return Record(
            collection_ref=collection.resource,
            record_id=record_id,
            values=normalized,
            record_version=1,
            created_at=timestamp,
            updated_at=timestamp,
        )

    def _is_null(
        self,
        context: GenerationContext,
        spec: FieldGeneratorSpec,
        field: FieldDefinition,
    ) -> bool:
        if context.identity_field or not field.nullable or spec.null_rate <= 0:
            return False
        return context.rng("null").random() < spec.null_rate

    @staticmethod
    def _many_count(context: GenerationContext, field: FieldDefinition) -> int:
        minimum = cast(int, field.constraints.get("minItems", 1))
        maximum = cast(int, field.constraints.get("maxItems", max(minimum, 3)))
        return context.rng("cardinality").randint(minimum, maximum)

    @staticmethod
    def _correlate(spec: CorrelationSpec, values: Mapping[str, object]) -> object:
        inputs = [values[name] for name in spec.inputs]
        if spec.kind == "copy":
            return inputs[0]
        if spec.kind == "linear":
            slope = spec.parameters.get("slope", 1.0)
            intercept = spec.parameters.get("intercept", 0.0)
            if not isinstance(slope, int | float) or not isinstance(intercept, int | float):
                raise InvalidSpec("linear correlation slope and intercept must be numeric")
            if not isinstance(inputs[0], int | float):
                raise InvalidSpec("linear correlation input must be numeric")
            return float(inputs[0]) * float(slope) + float(intercept)
        template = spec.parameters.get("template")
        if not isinstance(template, str):
            raise InvalidSpec("template correlation requires parameters.template")
        return template.format_map({name: values[name] for name in spec.inputs})

    def _check_control(self, cancellation: CancellationToken, started: float) -> None:
        if cancellation.cancelled:
            raise CancelledRun("synthetic run was cancelled")
        elapsed_ms = (time.monotonic() - started) * 1000
        if elapsed_ms > self.spec.bounds.timeout_ms:
            raise ExceededBudget("synthetic run exceeded bounds.timeoutMs")

    def _default_run_id(self) -> str:
        value = deterministic_uuid(
            self.spec.seed,
            self.spec.fingerprint,
            self.implementation_digest,
        ).replace("-", "")
        return f"synthetic-{value}"

    def _all_collections(self) -> tuple[CollectionSpec, ...]:
        return self.spec.collections + tuple(item.collection for item in self.spec.relations)

    def _execution_order(self) -> tuple[str, ...]:
        dependencies = {item.collection_id: set(item.depends_on) for item in self.spec.collections}
        dependencies.update(
            {
                item.collection.collection_id: {
                    *item.collection.depends_on,
                    item.source_collection,
                    item.target_collection,
                }
                for item in self.spec.relations
            }
        )
        result: list[str] = []
        remaining = dict(dependencies)
        while remaining:
            ready = sorted(name for name, values in remaining.items() if not values)
            if not ready:
                raise InvalidSpec("Collection dependency graph cannot be executed")
            result.extend(ready)
            for name in ready:
                del remaining[name]
            for values in remaining.values():
                values.difference_update(ready)
        return tuple(result)

    def _validate_sink(self, sink: RecordSink) -> None:
        expected = {
            OutputMode.RECORDS: {SinkKind.RECORDS, SinkKind.MEMORY},
            OutputMode.DATASET: {SinkKind.DATASET},
            OutputMode.STREAMING: {SinkKind.STREAMING},
        }[self.spec.output_mode]
        if sink.kind not in expected:
            raise InvalidSpec(
                "sink kind does not match SyntheticSpec outputMode",
                details={"sink": sink.kind.value, "outputMode": self.spec.output_mode.value},
            )


__all__ = [
    "IMPLEMENTATION_COORDINATE",
    "CancellationToken",
    "Generator",
    "RunProgress",
    "RunState",
    "SyntheticRun",
]
