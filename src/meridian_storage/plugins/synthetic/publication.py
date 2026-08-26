# SPDX-License-Identifier: Apache-2.0
"""Dataset manifest and explicit Configuration & Artifact publication seam."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Protocol, cast, runtime_checkable

from .canonical import (
    JsonValue,
    bounded_token,
    freeze_json,
    freeze_source_evidence,
    require_digest,
    sha256_digest,
)
from .errors import PublicationFailure
from .sinks import PartitionReceipt

DATASET_MANIFEST_VERSION = "meridian.synthetic.dataset-manifest.v1"


@dataclass(frozen=True, slots=True)
class DatasetManifest:
    run_id: str
    spec_id: str
    spec_version: str
    spec_fingerprint: str
    implementation_coordinate: str
    implementation_digest: str
    schema_fingerprints: Mapping[str, str]
    partitions: tuple[PartitionReceipt, ...]
    validation_digest: str
    source_evidence: tuple[Mapping[str, JsonValue], ...] = ()
    format_version: str = DATASET_MANIFEST_VERSION

    def __post_init__(self) -> None:
        if self.format_version != DATASET_MANIFEST_VERSION:
            raise ValueError("unsupported dataset manifest formatVersion")
        for name in ("run_id", "spec_id", "implementation_coordinate"):
            object.__setattr__(self, name, bounded_token(getattr(self, name), name))
        for name in (
            "spec_fingerprint",
            "implementation_digest",
            "validation_digest",
        ):
            object.__setattr__(self, name, require_digest(getattr(self, name), name))
        schemas = {
            name: require_digest(value, name) for name, value in self.schema_fingerprints.items()
        }
        object.__setattr__(
            self, "schema_fingerprints", MappingProxyType(dict(sorted(schemas.items())))
        )
        object.__setattr__(
            self,
            "partitions",
            tuple(
                replace(item, skipped=False)
                for item in sorted(self.partitions, key=lambda item: item.partition_id)
            ),
        )
        object.__setattr__(self, "source_evidence", freeze_source_evidence(self.source_evidence))

    @property
    def artifact_digest(self) -> str:
        return sha256_digest(
            cast(
                JsonValue,
                {
                    "manifest": self.to_dict(include_artifact_digest=False),
                    "partitionDigests": [item.digest for item in self.partitions],
                },
            )
        )

    def to_dict(self, *, include_artifact_digest: bool = True) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {
            "formatVersion": self.format_version,
            "runId": self.run_id,
            "spec": {
                "id": self.spec_id,
                "version": self.spec_version,
                "fingerprint": self.spec_fingerprint,
            },
            "implementation": {
                "coordinate": self.implementation_coordinate,
                "digest": self.implementation_digest,
            },
            "schemas": dict(self.schema_fingerprints),
            "partitions": [item.to_dict() for item in self.partitions],
            "validationDigest": self.validation_digest,
            "sources": [dict(item) for item in self.source_evidence],
        }
        if include_artifact_digest:
            result["artifactDigest"] = self.artifact_digest
        return result


@dataclass(frozen=True, slots=True)
class ArtifactPublication:
    artifact_ref: str
    digest: str
    metadata: Mapping[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.artifact_ref, str) or not self.artifact_ref:
            raise ValueError("artifact_ref must be non-empty")
        object.__setattr__(self, "digest", require_digest(self.digest, "artifact digest"))
        object.__setattr__(
            self,
            "metadata",
            cast(Mapping[str, JsonValue], freeze_json(cast(JsonValue, self.metadata))),
        )

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "artifactRef": self.artifact_ref,
            "digest": self.digest,
            "metadata": dict(self.metadata),
        }


@runtime_checkable
class ArtifactPublisher(Protocol):
    """Owning-process bridge to the Configuration & Artifact plugin contract."""

    def publish(
        self,
        manifest: DatasetManifest,
        partitions: Mapping[str, bytes],
    ) -> ArtifactPublication: ...


class InMemoryArtifactPublisher:
    """Conformance publisher that models the public artifact hand-off in memory."""

    def __init__(self) -> None:
        self.publications: dict[str, tuple[DatasetManifest, Mapping[str, bytes]]] = {}

    def publish(
        self,
        manifest: DatasetManifest,
        partitions: Mapping[str, bytes],
    ) -> ArtifactPublication:
        expected = {item.partition_id: item.digest for item in manifest.partitions}
        if set(partitions) != set(expected):
            raise PublicationFailure("artifact publisher received an incomplete partition set")
        for partition_id, payload in partitions.items():
            if sha256_digest(payload) != expected[partition_id]:
                raise PublicationFailure(
                    "artifact publisher received a partition digest mismatch",
                    details={"partitionId": partition_id},
                )
        existing = self.publications.get(manifest.run_id)
        if existing is not None and existing[0].artifact_digest != manifest.artifact_digest:
            raise PublicationFailure("artifact publication idempotency conflict")
        frozen_payloads = MappingProxyType(
            {name: bytes(value) for name, value in sorted(partitions.items())}
        )
        self.publications[manifest.run_id] = (manifest, frozen_payloads)
        return ArtifactPublication(
            artifact_ref=f"artifact:synthetic.{manifest.run_id}",
            digest=manifest.artifact_digest,
            metadata={"formatVersion": manifest.format_version},
        )


__all__ = [
    "DATASET_MANIFEST_VERSION",
    "ArtifactPublication",
    "ArtifactPublisher",
    "DatasetManifest",
    "InMemoryArtifactPublisher",
]
