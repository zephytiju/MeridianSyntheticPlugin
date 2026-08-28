<!-- SPDX-License-Identifier: Apache-2.0 -->

# Architecture and authority boundaries

The package implements Synthetic Data Generation LLD revision 34 against the
corrected Meridian HLD revision 109, Catalog/Public Interfaces revision 70, Engine
Adapters revision 24, Kafka Streaming LLD revision 6, and MeridianConstructs
revision 45.

The implementation is an embeddable Python library. It owns no Catalog and
runs no network service. Its dependency graph contains only released Meridian
V1 distributions and ordinary Python tooling; it has no Kafka client,
storage-engine SDK, Adapter, or infrastructure dependency.

Execution has four stages:

1. `SyntheticSpecV1` strictly parses and fingerprints schemas, generator pins,
   source projections, relations, validation rules, output mode, and bounds.
2. `GeneratorRegistry` resolves deterministic implementations and derives an
   independent random stream for each collection, ordinal, field, element, and
   purpose. A topological collection plan makes relation endpoints available
   before edge fixtures.
3. Fixed-size partitions are schema-validated and passed to an explicit sink.
   Record and Event sinks produce mapping-first serialized Expressions through
   `Meridian`; they never instantiate an Adapter or inspect an Engine.
4. Incremental validation and evidence produce canonical digests. Dataset
   partitions can be handed to an `ArtifactPublisher`, the owning process's
   bridge to the Configuration & Artifact plugin.

The run lifecycle is `PLANNED`, `RUNNING`, `VALIDATING`, then `PUBLISHED`, with
terminal `FAILED` and `CANCELLED` paths. Partition IDs and idempotency keys are
stable, and a `CheckpointStore` allows safe retry without changing payload
digests. Cleanup is opt-in, best-effort, and limited to staging targets.

Tenancy, principal, scopes, tracing, request identity, deadlines, and policy
remain in the caller-provided Meridian `OperationContext`. Provisioning,
selection, identity/ACL, migrations, recovery, scaling, and lifecycle remain
the authority of Platform/Vangu IaC through MeridianConstructs.

The repository publishes exactly one distribution,
`meridian-storage-plugin-synthetic`, and contributes only the non-overlapping
`meridian_storage.plugins.synthetic` namespace. Core discovers its `synthetic`
PluginFactory and SchemaProvider entry points. The provider declares logical
`structured:synthetic.specs` and `structured:synthetic.run-evidence` Resources;
the in-process repository uses only public `structured.get` and `structured.put`
Expressions for immutable registration and exact lookup.
