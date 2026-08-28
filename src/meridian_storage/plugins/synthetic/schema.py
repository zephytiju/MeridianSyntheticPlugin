# SPDX-License-Identifier: Apache-2.0
"""Logical Schema and Resource contribution for Synthetic V1."""

from __future__ import annotations

from collections.abc import Iterable

from meridian_storage.registry.resources import (
    CapabilityRequirement,
    NamespaceDefinition,
    ResourceBundle,
    ResourceDefinition,
)
from meridian_storage.semantics import (
    CatalogName,
    FieldDefinition,
    LogicalKind,
    LogicalType,
    SchemaDocument,
    SchemaReference,
    SemanticKind,
)

from ._version import __version__
from .repository import SyntheticResources

_CONTRACT_VERSION = "1.0.0"


def _field(name: str, kind: LogicalKind, *, nullable: bool = False) -> FieldDefinition:
    return FieldDefinition(name, LogicalType(kind), nullable=nullable, mutable=False)


def _document(
    name: str,
    fields: Iterable[FieldDefinition],
    identity: tuple[str, ...],
) -> SchemaDocument:
    return SchemaDocument(
        ref=SchemaReference(CatalogName.STRUCTURED, "synthetic", name, _CONTRACT_VERSION),
        semantic_kind=SemanticKind.RELATIONAL,
        fields=tuple(fields),
        identity=identity,
        consistency="strong",
        retention_label="synthetic-evidence",
        extensions={"org.meridian.synthetic/contract": _CONTRACT_VERSION},
        compatibility={"mode": "backward"},
    )


def spec_resource_schema() -> SchemaDocument:
    return _document(
        "specs",
        (
            _field("formatVersion", LogicalKind.STRING),
            _field("specId", LogicalKind.STRING),
            _field("specVersion", LogicalKind.STRING),
            _field("fingerprint", LogicalKind.STRING),
            _field("document", LogicalKind.JSON),
        ),
        ("specId", "specVersion"),
    )


def run_evidence_resource_schema() -> SchemaDocument:
    return _document(
        "run-evidence",
        (
            _field("formatVersion", LogicalKind.STRING),
            _field("runId", LogicalKind.STRING),
            _field("state", LogicalKind.STRING),
            _field("fingerprint", LogicalKind.STRING),
            _field("document", LogicalKind.JSON),
        ),
        ("runId", "state"),
    )


def synthetic_schemas() -> tuple[SchemaDocument, ...]:
    return (spec_resource_schema(), run_evidence_resource_schema())


def _requirements(*methods: str) -> tuple[CapabilityRequirement, ...]:
    return tuple(
        CapabilityRequirement(f"meridian.structured.{method}", "1.0.0") for method in methods
    )


class SyntheticSchemaProvider:
    @property
    def provider_id(self) -> str:
        return "synthetic"

    @property
    def provider_contract_version(self) -> str:
        return _CONTRACT_VERSION

    def load(self) -> ResourceBundle:
        resources = SyntheticResources()
        documents = synthetic_schemas()
        schemas = tuple(document.to_core_definition() for document in documents)
        return ResourceBundle(
            provider_id=self.provider_id,
            provider_version=__version__,
            provider_contract_version=self.provider_contract_version,
            namespaces=(
                NamespaceDefinition(
                    "structured",
                    "synthetic",
                    labels={"plugin": "synthetic", "lifecycleOwner": "consumer"},
                ),
            ),
            schemas=schemas,
            resources=(
                ResourceDefinition(
                    resources.specs,
                    "relational",
                    schemas[0].ref,
                    labels={"plugin": "synthetic", "recordType": "spec"},
                    requirements=_requirements("get", "put"),
                    related_resources=(resources.run_evidence,),
                ),
                ResourceDefinition(
                    resources.run_evidence,
                    "relational",
                    schemas[1].ref,
                    labels={"plugin": "synthetic", "recordType": "run-evidence"},
                    requirements=_requirements("get", "put"),
                    related_resources=(resources.specs,),
                ),
            ),
            extensions={
                "distribution": "meridian-storage-plugin-synthetic",
                "catalogsOwned": [],
                "catalogsUsed": ["structured", "evidence", "streaming"],
                "design": {
                    "hldRevision": 109,
                    "catalogRevision": 70,
                    "adapterRevision": 24,
                    "kafkaStreamingRevision": 6,
                    "constructsRevision": 45,
                    "syntheticLldRevision": 34,
                },
            },
        )


__all__ = [
    "SyntheticSchemaProvider",
    "run_evidence_resource_schema",
    "spec_resource_schema",
    "synthetic_schemas",
]
