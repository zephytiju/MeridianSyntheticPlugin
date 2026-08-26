# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from conftest import basic_spec

from meridian_storage.plugins.synthetic import (
    ArtifactPublication,
    CanonicalDatasetSink,
    Generator,
    InMemoryPartitionStore,
    MemorySink,
    PublicationFailure,
    RunState,
    SyntheticPluginFactory,
    plugin_manifest,
)


def test_public_factory_and_progress_mapping() -> None:
    factory = SyntheticPluginFactory()
    generator = factory.create(basic_spec())
    run = generator.run(MemorySink())
    assert run.progress.to_dict() == {
        "generatedRecords": 9,
        "writtenPartitions": 3,
        "generatedBytes": run.progress.generated_bytes,
    }
    assert factory(basic_spec()).implementation_digest == generator.implementation_digest
    assert plugin_manifest()["service"] is False
    with pytest.raises(PublicationFailure, match="dataset output"):
        _ = run.manifest


def test_run_argument_and_publication_failures_are_bounded() -> None:
    generator = Generator(basic_spec())
    with pytest.raises(ValueError, match="workers"):
        generator.run(MemorySink(), workers=0)
    with pytest.raises(ValueError, match="run_id"):
        generator.run(MemorySink(), run_id="x" * 257)

    spec = basic_spec(output_mode="dataset", count=1)
    sink = CanonicalDatasetSink(
        store=InMemoryPartitionStore(max_bytes=spec.bounds.max_memory_bytes)
    )
    run = Generator(spec).run(sink)

    class ExplodingPublisher:
        def publish(self, manifest, partitions):
            raise RuntimeError("offline")

    with pytest.raises(PublicationFailure, match="rejected"):
        run.publish(ExplodingPublisher())
    run.state = RunState.FAILED
    with pytest.raises(PublicationFailure, match="successful"):
        run.publish(ExplodingPublisher())


def test_artifact_publication_validates_identity_and_digest() -> None:
    with pytest.raises(ValueError, match="artifact_ref"):
        ArtifactPublication("", "sha256:" + "0" * 64)
    with pytest.raises(ValueError, match="fingerprint"):
        ArtifactPublication("artifact:test", "bad")
