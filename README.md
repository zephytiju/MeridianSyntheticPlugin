<!-- SPDX-License-Identifier: Apache-2.0 -->

# Meridian Synthetic Plugin

`meridian-storage-plugin-synthetic` is the in-process, deterministic synthetic-data
generation plugin for Meridian V1. It generates schema-valid Records,
relation-aware fixtures, canonical datasets, and provider-neutral Events. Every
write goes through released Meridian public contracts.

This repository contains exactly one Python distribution and is licensed under
the Apache License, Version 2.0.

## Install

The plugin requires the released Meridian V1 Core, Semantics, Query, Evidence,
and Streaming distributions. After those artifacts are available:

```console
python -m pip install meridian-storage-plugin-synthetic
```

## Use

```python
from meridian_storage.plugins.synthetic import Generator, MemorySink, SyntheticSpec

spec = SyntheticSpec.load(spec_document)
sink = MemorySink(max_bytes=spec.bounds.max_memory_bytes)
run = Generator(spec).run(sink, workers=4)

assert run.validation.passed
records = sink.records(run.run_id)
```

`SyntheticSpec` pins the package coordinate, optional implementation digest,
schema fingerprints, seed, locale set, time interval, counts, generators,
relations, validation rules, output mode, and execution bounds. Random streams
are derived from logical coordinates, so partitioning, worker count, and run ID
do not change generated values.

Available sinks are:

- `MemorySink` for bounded tests and fixtures;
- `MeridianRecordSink` for public `structured.put` writes;
- `MeridianStreamingSink` for public `streaming.publish_batch` writes; and
- `CanonicalDatasetSink` for canonical NDJSON partitions followed by an
  explicit `ArtifactPublisher` hand-off.

Direct Record writes require a `DirectWritePolicy`. Production targets require
an explicit approval flag from the owning process. Dataset publication never
uses the Object Catalog directly; the caller supplies the Configuration &
Artifact plugin bridge.

Source-backed generation accepts only an explicit, projected
`structured.query` Expression and a digest-pinned deterministic transform. Run
evidence includes source boundaries, query fingerprints, transform digests, and
row counts, but never source values.

Meridian Core discovers the package through the `synthetic` entries in
`meridian_storage.plugins` and `meridian_storage.schemas`. The plugin facade
exposes a public-Expression repository for immutable `SyntheticSpec` and
`RunEvidence` resources; it creates no registry service and never receives an
Adapter or Engine client.

## Contracts and support

Serialized schemas are in [`contracts`](contracts), design alignment is in
[`docs/architecture.md`](docs/architecture.md), and the compatibility pin is in
[`compatibility.json`](compatibility.json). Python 3.12 through 3.14 is
supported. Report vulnerabilities using [`SECURITY.md`](SECURITY.md).

## Build and release (Jumbo)

This repository is jumbo-managed (Jumbo Build & Versioning Standard,
section 3.5): resolution, builds, and releases run through jumbo, never
ad-hoc pip/uv installs.

```sh
jumbo lock   # resolve internal packages from the JumboIndex, third-party from PyPI
jumbo build  # build + tests at the resolved closure
```

The internal dependencies (`meridian-storage-core`, `meridian-storage-evidence`, `meridian-storage-query`, `meridian-storage-semantics`, `meridian-storage-streaming`) are resolved from the JumboIndex;
the lock records the exact promoted build of each. Consumers likewise
resolve this package (`meridian-storage-plugin-synthetic`) from the JumboIndex. Releases are dispatch-only through `.github/workflows/jumbo-publish.yml`;
as a public package, external publication is driven by the jumbo-computed
version, and every artifact's SHA-256 is recorded in the append-only
JumboIndex.
