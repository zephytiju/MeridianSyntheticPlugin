# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from conftest import basic_spec

from meridian_storage.plugins.synthetic import (
    CanonicalDatasetSink,
    Generator,
    InMemoryArtifactPublisher,
    InMemoryPartitionStore,
    MemorySink,
    OutputMode,
    PublicationFailure,
)


def test_dataset_partitions_manifest_and_publication_are_canonical() -> None:
    spec = basic_spec(output_mode=OutputMode.DATASET, count=5, batch_size=2)
    store = InMemoryPartitionStore(max_bytes=spec.bounds.max_memory_bytes)
    sink = CanonicalDatasetSink(store=store)
    run = Generator(spec).run(sink)
    assert len(run.receipts) == 3
    assert all(store.get(run.run_id, item.partition_id).endswith(b"\n") for item in run.receipts)
    manifest = run.manifest
    assert manifest.artifact_digest.startswith("sha256:")
    resumed = Generator(spec).run(sink, run_id=run.run_id)
    assert resumed.manifest.artifact_digest == manifest.artifact_digest
    assert all(not item.skipped for item in resumed.manifest.partitions)
    publisher = InMemoryArtifactPublisher()
    evidence_count = len(run.evidence)
    publication = run.publish(publisher)
    assert publication.digest == manifest.artifact_digest
    assert run.publish(publisher) == publication
    assert len(run.evidence) == evidence_count + 1
    assert run.evidence[-1].artifact_digest == publication.digest


def test_dataset_store_and_publication_enforce_bounds_and_digests() -> None:
    store = InMemoryPartitionStore(max_bytes=2)
    with pytest.raises(Exception, match="memory bound"):
        store.put("run", "part", b"too-large", "sha256:" + "0" * 64)
    with pytest.raises(KeyError):
        store.get("run", "missing")

    spec = basic_spec(output_mode=OutputMode.DATASET, count=1)
    sink = CanonicalDatasetSink(
        store=InMemoryPartitionStore(max_bytes=spec.bounds.max_memory_bytes)
    )
    run = Generator(spec).run(sink)

    class BadPublisher:
        def publish(self, manifest, partitions):
            result = InMemoryArtifactPublisher().publish(manifest, partitions)
            return type(result)(result.artifact_ref, "sha256:" + "0" * 64)

    with pytest.raises(PublicationFailure, match="unexpected digest"):
        run.publish(BadPublisher())


def test_non_dataset_run_cannot_publish() -> None:
    run = Generator(basic_spec()).run(MemorySink())
    with pytest.raises(PublicationFailure, match="dataset"):
        run.publish(InMemoryArtifactPublisher())
