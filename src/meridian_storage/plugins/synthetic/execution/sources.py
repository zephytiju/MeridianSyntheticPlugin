# SPDX-License-Identifier: Apache-2.0
"""Explicit, projected source-backed generation through public Meridian Query."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol, cast, runtime_checkable

from meridian_storage import Expression, Meridian, OperationContext, OperationResult
from meridian_storage.semantics import FrozenJson

from ..canonical import (
    JsonValue,
    bounded_token,
    canonical_json_bytes,
    freeze_json,
    require_digest,
    sha256_digest,
)
from ..errors import ExceededBudget, SourcePolicyViolation
from ..spec import SourceSpec


@runtime_checkable
class SourceReader(Protocol):
    """Bounded reader that consumes one already-approved source specification."""

    def read(self, source: SourceSpec) -> Sequence[Mapping[str, object]]: ...


class _Runtime(Protocol):
    def context(self, context: OperationContext) -> AbstractContextManager[OperationContext]: ...

    def execute(self, expression: Expression) -> OperationResult: ...


class MeridianSourceReader:
    """Execute an explicit Query Expression using only the Meridian Core facade."""

    def __init__(self, meridian: Meridian, context: OperationContext) -> None:
        self._meridian = cast(_Runtime, meridian)
        self._context = context

    def read(self, source: SourceSpec) -> Sequence[Mapping[str, object]]:
        with self._meridian.context(self._context):
            result = self._meridian.execute(source.query)
        data = result.data
        raw_rows = data.get("items", data.get("records"))
        if not isinstance(raw_rows, Sequence) or isinstance(raw_rows, str | bytes | bytearray):
            raise SourcePolicyViolation(
                "source Query result must contain a bounded items or records array"
            )
        if len(raw_rows) > source.max_rows:
            raise SourcePolicyViolation("source Query returned more than approved maxRows")
        rows: list[Mapping[str, object]] = []
        for raw in raw_rows:
            if not isinstance(raw, Mapping):
                raise SourcePolicyViolation("source Query rows must be objects")
            values = raw.get("values", raw)
            if not isinstance(values, Mapping):
                raise SourcePolicyViolation("source Query Record values must be an object")
            missing = set(source.projection) - set(values)
            unknown = set(values) - set(source.projection)
            if missing or unknown:
                raise SourcePolicyViolation(
                    "source row does not match the approved projection",
                    details={
                        "missing": ",".join(sorted(str(item) for item in missing)) or "none",
                        "unknown": ",".join(sorted(str(item) for item in unknown)) or "none",
                    },
                )
            rows.append({name: values[name] for name in source.projection})
        return tuple(rows)


SourceTransform = Callable[[Mapping[str, object], int], Mapping[str, object]]


@dataclass(frozen=True, slots=True)
class SourceTransformation:
    transformation_id: str
    implementation_digest: str
    transform: SourceTransform
    deterministic: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "transformation_id",
            bounded_token(self.transformation_id, "source transformation id"),
        )
        object.__setattr__(
            self,
            "implementation_digest",
            require_digest(self.implementation_digest, "source transformation digest"),
        )
        if not callable(self.transform):
            raise TypeError("source transformation must be callable")


class SourceTransformationRegistry:
    def __init__(self) -> None:
        self._items: dict[str, SourceTransformation] = {}
        self._sealed = False

    def register(self, value: SourceTransformation) -> None:
        if self._sealed:
            raise RuntimeError("source transformation registry is sealed")
        if value.transformation_id in self._items:
            raise SourcePolicyViolation(
                f"duplicate source transformation {value.transformation_id!r}"
            )
        self._items[value.transformation_id] = value

    def seal(self) -> SourceTransformationRegistry:
        self._sealed = True
        return self

    def resolve(self, source: SourceSpec) -> SourceTransformation:
        try:
            value = self._items[source.transformation_id]
        except KeyError as exc:
            raise SourcePolicyViolation(
                f"unknown source transformation {source.transformation_id!r}"
            ) from exc
        if not value.deterministic or value.implementation_digest != source.transformation_digest:
            raise SourcePolicyViolation(
                f"source transformation {source.transformation_id!r} differs from its digest pin"
            )
        return value

    @property
    def implementation_digest(self) -> str:
        return sha256_digest(
            cast(
                JsonValue,
                [
                    {
                        "id": item.transformation_id,
                        "implementationDigest": item.implementation_digest,
                        "deterministic": item.deterministic,
                    }
                    for item in sorted(
                        self._items.values(), key=lambda value: value.transformation_id
                    )
                ],
            )
        )


def _identity(row: Mapping[str, object], ordinal: int) -> Mapping[str, object]:
    del ordinal
    return dict(row)


IDENTITY_TRANSFORMATION_DIGEST = sha256_digest(
    cast(JsonValue, {"id": "core.identity@1", "algorithm": "projected-copy-v1"})
)


def builtin_source_transformations() -> SourceTransformationRegistry:
    registry = SourceTransformationRegistry()
    registry.register(
        SourceTransformation(
            transformation_id="core.identity@1",
            implementation_digest=IDENTITY_TRANSFORMATION_DIGEST,
            transform=_identity,
        )
    )
    return registry.seal()


@dataclass(frozen=True, slots=True)
class SourceSnapshot:
    source_id: str
    query_fingerprint: str
    source_boundary: str
    transformation_id: str
    transformation_digest: str
    rows: tuple[Mapping[str, FrozenJson], ...]

    @property
    def row_count(self) -> int:
        return len(self.rows)

    @property
    def memory_bytes(self) -> int:
        return sum(len(canonical_json_bytes(cast(JsonValue, row))) for row in self.rows)

    @property
    def row_digest(self) -> str:
        return sha256_digest(cast(JsonValue, list(self.rows)))

    def evidence(self) -> Mapping[str, JsonValue]:
        """Return reproducibility metadata that deliberately excludes raw row values."""

        return MappingProxyType(
            {
                "sourceId": self.source_id,
                "queryFingerprint": self.query_fingerprint,
                "sourceBoundary": self.source_boundary,
                "transformationId": self.transformation_id,
                "transformationDigest": self.transformation_digest,
                "rowCount": self.row_count,
                "rowDigest": self.row_digest,
            }
        )


def materialize_sources(
    sources: Sequence[SourceSpec],
    *,
    reader: SourceReader | None,
    transformations: SourceTransformationRegistry,
    max_memory_bytes: int | None = None,
) -> Mapping[str, SourceSnapshot]:
    if sources and reader is None:
        raise SourcePolicyViolation("source-backed generation requires an explicit SourceReader")
    snapshots: dict[str, SourceSnapshot] = {}
    memory_bytes = 0
    for source in sources:
        if reader is None:
            raise SourcePolicyViolation("source-backed generation requires a SourceReader")
        transformation = transformations.resolve(source)
        raw_rows = reader.read(source)
        if not raw_rows:
            raise SourcePolicyViolation(f"source {source.source_id!r} returned no approved rows")
        if len(raw_rows) > source.max_rows:
            raise SourcePolicyViolation(f"source {source.source_id!r} exceeded maxRows")
        try:
            ordered_rows = sorted(
                raw_rows, key=lambda row: canonical_json_bytes(cast(JsonValue, row))
            )
        except (TypeError, ValueError) as exc:
            raise SourcePolicyViolation("SourceReader returned non-JSON row data") from exc
        normalized: list[Mapping[str, FrozenJson]] = []
        for ordinal, row in enumerate(ordered_rows):
            if set(row) != set(source.projection):
                raise SourcePolicyViolation("SourceReader returned a row outside its projection")
            transformed = transformation.transform(row, ordinal)
            try:
                frozen = freeze_json(cast(JsonValue, transformed))
            except (TypeError, ValueError) as exc:
                raise SourcePolicyViolation("source transformation returned non-JSON data") from exc
            if not isinstance(frozen, Mapping):
                raise SourcePolicyViolation("source transformation must return an object")
            frozen_row = cast(Mapping[str, FrozenJson], frozen)
            memory_bytes += len(canonical_json_bytes(cast(JsonValue, frozen_row)))
            if max_memory_bytes is not None and memory_bytes > max_memory_bytes:
                raise ExceededBudget("materialized sources exceed maxMemoryBytes")
            normalized.append(frozen_row)
        snapshots[source.source_id] = SourceSnapshot(
            source_id=source.source_id,
            query_fingerprint=source.query_fingerprint,
            source_boundary=source.resource.canonical,
            transformation_id=source.transformation_id,
            transformation_digest=source.transformation_digest,
            rows=tuple(normalized),
        )
    return MappingProxyType(dict(sorted(snapshots.items())))


__all__ = [
    "IDENTITY_TRANSFORMATION_DIGEST",
    "MeridianSourceReader",
    "SourceReader",
    "SourceSnapshot",
    "SourceTransformation",
    "SourceTransformationRegistry",
    "builtin_source_transformations",
    "materialize_sources",
]
