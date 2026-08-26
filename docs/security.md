<!-- SPDX-License-Identifier: Apache-2.0 -->

# Security model

Synthetic output can contain sensitive-looking values and must be handled using
the same tenant, policy, retention, and evidence controls as any other Meridian
data. The library does not infer permission from a schema.

- An owning process supplies `OperationContext` for all Meridian calls.
- Production Record targets require explicit `production_approved=True`.
- Failure cleanup is allowed only for staging targets.
- Source reads require an exact `structured.query`, explicit direct-field
  projection, row bound, and digest-pinned deterministic transformation.
- Evidence contains fingerprints, logical boundaries, counts, states, and safe
  bounded error codes; it excludes source values and generated field values.
- Memory and dataset stores are bounded and intended as reference
  implementations. Applications handling sensitive fixtures should supply an
  approved bounded store.

The library neither provisions nor accesses private storage. Infrastructure,
secrets, identities, ACLs, and data lifecycle remain outside this package.
