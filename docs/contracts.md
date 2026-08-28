<!-- SPDX-License-Identifier: Apache-2.0 -->

# Public contracts

The canonical distribution is `meridian-storage-plugin-synthetic` and the
stable import root is `meridian_storage.plugins.synthetic`.

- `SyntheticSpec` / `SyntheticSpecV1` is the strict immutable input model.
- `Generator(spec).run(sink, ...)` executes bounded deterministic generation.
- `SyntheticRun.publish(publisher)` publishes dataset output through the
  explicit Configuration & Artifact seam.
- `GeneratorRegistry` and `SourceTransformationRegistry` allow versioned,
  digest-pinned extensions before execution.
- `RecordSink`, `SourceReader`, `EvidenceHook`, and `ArtifactPublisher` are the
  application integration protocols.
- `SyntheticPluginFactory` implements the Meridian Core V1 PluginFactory SPI;
  `SyntheticSchemaProvider` contributes the plugin-owned logical Resources.
- `SyntheticRepository` registers and retrieves immutable `SyntheticSpec` and
  state-specific `RunEvidence` documents through public structured Expressions.

The JSON contract versions are `meridian.synthetic.spec.v1`,
`meridian.synthetic.dataset-manifest.v1`, and the stable
`MERIDIAN_SYNTHETIC_*` error envelope. Unknown input fields are rejected.

Compatibility is additive within major version 1. Generator implementation
changes require a new generator ID or digest. The canonical manifest includes
every partition digest, schema fingerprint, implementation digest, validation
digest, and source-policy fingerprint needed to reproduce and compare a run.
