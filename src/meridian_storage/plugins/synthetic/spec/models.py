# SPDX-License-Identifier: Apache-2.0
"""Strict immutable ``meridian.synthetic.spec.v1`` models."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dc_field
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any, cast

from meridian_storage import Expression
from meridian_storage.semantics import (
    RelationProfile,
    ResourceReference,
    SchemaDocument,
    validate_schema,
)

from ..canonical import (
    JsonValue,
    bounded_token,
    freeze_json,
    require_digest,
    sha256_digest,
    thaw_json,
)
from ..errors import InvalidSpec, SourcePolicyViolation

SPEC_FORMAT_VERSION = "meridian.synthetic.spec.v1"
_VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.-]+)?$")
_LOCALE_RE = re.compile(r"^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{2,8})*$")


def _mapping(value: object, field_path: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise InvalidSpec(f"{field_path} must be an object", field_path=field_path)
    return cast(Mapping[str, object], value)


def _array(value: object, field_path: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes | bytearray):
        raise InvalidSpec(f"{field_path} must be an array", field_path=field_path)
    return cast(Sequence[object], value)


def _keys(
    value: Mapping[str, object],
    *,
    required: set[str],
    optional: set[str] | None = None,
    field_path: str,
) -> None:
    allowed = required | (optional or set())
    missing = required - set(value)
    unknown = set(value) - allowed
    if missing or unknown:
        raise InvalidSpec(
            f"{field_path} contains unknown or missing fields",
            field_path=field_path,
            details={
                "missing": ",".join(sorted(missing)) or "none",
                "unknown": ",".join(sorted(unknown)) or "none",
            },
        )


def _positive(value: object, field_path: str, *, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise InvalidSpec(f"{field_path} must be a positive integer", field_path=field_path)
    if maximum is not None and value > maximum:
        raise InvalidSpec(f"{field_path} exceeds its supported maximum", field_path=field_path)
    return value


def _timestamp(value: object, field_path: str) -> str:
    if not isinstance(value, str):
        raise InvalidSpec(f"{field_path} must be an RFC 3339 string", field_path=field_path)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidSpec(f"{field_path} is not a timestamp", field_path=field_path) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise InvalidSpec(f"{field_path} must include an offset", field_path=field_path)
    return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _json_mapping(value: object, field_path: str) -> Mapping[str, JsonValue]:
    mapping = _mapping(value, field_path)
    try:
        frozen = freeze_json(cast(JsonValue, mapping))
    except (TypeError, ValueError) as exc:
        raise InvalidSpec(f"{field_path} is not canonical JSON", field_path=field_path) from exc
    return cast(Mapping[str, JsonValue], frozen)


class OutputMode(StrEnum):
    RECORDS = "records"
    DATASET = "dataset"
    STREAMING = "streaming"


@dataclass(frozen=True, slots=True)
class TimeBounds:
    start: str
    end: str

    def __post_init__(self) -> None:
        start = _timestamp(self.start, "timeBounds.start")
        end = _timestamp(self.end, "timeBounds.end")
        if end < start:
            raise InvalidSpec("timeBounds.end precedes start", field_path="timeBounds.end")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> TimeBounds:
        _keys(value, required={"start", "end"}, field_path="timeBounds")
        return cls(cast(str, value["start"]), cast(str, value["end"]))

    def to_dict(self) -> dict[str, str]:
        return {"start": self.start, "end": self.end}


@dataclass(frozen=True, slots=True)
class ExecutionBounds:
    max_records: int
    max_bytes: int
    timeout_ms: int
    max_memory_bytes: int
    batch_size: int

    def __post_init__(self) -> None:
        for name in ("max_records", "max_bytes", "timeout_ms", "max_memory_bytes", "batch_size"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise InvalidSpec(f"bounds.{name} must be a positive integer")
        if self.batch_size > self.max_records:
            raise InvalidSpec("bounds.batchSize cannot exceed maxRecords")
        if self.max_memory_bytes > self.max_bytes * 8:
            raise InvalidSpec("bounds.maxMemoryBytes is implausibly larger than maxBytes")

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> ExecutionBounds:
        required = {"maxRecords", "maxBytes", "timeoutMs", "maxMemoryBytes", "batchSize"}
        _keys(value, required=required, field_path="bounds")
        return cls(
            _positive(value["maxRecords"], "bounds.maxRecords", maximum=100_000_000),
            _positive(value["maxBytes"], "bounds.maxBytes"),
            _positive(value["timeoutMs"], "bounds.timeoutMs"),
            _positive(value["maxMemoryBytes"], "bounds.maxMemoryBytes"),
            _positive(value["batchSize"], "bounds.batchSize", maximum=10_000),
        )

    def to_dict(self) -> dict[str, int]:
        return {
            "maxRecords": self.max_records,
            "maxBytes": self.max_bytes,
            "timeoutMs": self.timeout_ms,
            "maxMemoryBytes": self.max_memory_bytes,
            "batchSize": self.batch_size,
        }


@dataclass(frozen=True, slots=True)
class ImplementationPin:
    coordinate: str
    required_digest: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "coordinate", bounded_token(self.coordinate, "implementation.coordinate")
        )
        if self.required_digest is not None:
            object.__setattr__(
                self,
                "required_digest",
                require_digest(self.required_digest, "implementation.requiredDigest"),
            )

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> ImplementationPin:
        _keys(
            value,
            required={"coordinate"},
            optional={"requiredDigest"},
            field_path="implementation",
        )
        return cls(cast(str, value["coordinate"]), cast(str | None, value.get("requiredDigest")))

    def to_dict(self) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {"coordinate": self.coordinate}
        if self.required_digest is not None:
            result["requiredDigest"] = self.required_digest
        return result


@dataclass(frozen=True, slots=True)
class FieldGeneratorSpec:
    generator_id: str
    config: Mapping[str, JsonValue] = dc_field(default_factory=dict)
    null_rate: float = 0.0
    expected_digest: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "generator_id", bounded_token(self.generator_id, "field generator id")
        )
        object.__setattr__(
            self, "config", cast(Mapping[str, JsonValue], freeze_json(cast(JsonValue, self.config)))
        )
        if (
            isinstance(self.null_rate, bool)
            or not isinstance(self.null_rate, int | float)
            or not 0.0 <= float(self.null_rate) <= 1.0
        ):
            raise InvalidSpec("field generator nullRate must be between zero and one")
        object.__setattr__(self, "null_rate", float(self.null_rate))
        if self.expected_digest is not None:
            object.__setattr__(
                self,
                "expected_digest",
                require_digest(self.expected_digest, "field generator expectedDigest"),
            )

    @classmethod
    def from_mapping(cls, value: Mapping[str, object], field_path: str) -> FieldGeneratorSpec:
        _keys(
            value,
            required={"generator"},
            optional={"config", "nullRate", "expectedDigest"},
            field_path=field_path,
        )
        config = _json_mapping(value.get("config", {}), f"{field_path}.config")
        return cls(
            generator_id=cast(str, value["generator"]),
            config=config,
            null_rate=cast(float, value.get("nullRate", 0.0)),
            expected_digest=cast(str | None, value.get("expectedDigest")),
        )

    def to_dict(self) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {
            "generator": self.generator_id,
            "config": thaw_json(cast(JsonValue, self.config)),
            "nullRate": self.null_rate,
        }
        if self.expected_digest is not None:
            result["expectedDigest"] = self.expected_digest
        return result


@dataclass(frozen=True, slots=True)
class CorrelationSpec:
    field: str
    kind: str
    inputs: tuple[str, ...]
    parameters: Mapping[str, JsonValue] = dc_field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "field", bounded_token(self.field, "correlation field", 256))
        if self.kind not in {"copy", "linear", "template"}:
            raise InvalidSpec(f"unsupported correlation kind {self.kind!r}")
        inputs = tuple(bounded_token(item, "correlation input", 256) for item in self.inputs)
        if not inputs or len(set(inputs)) != len(inputs) or self.field in inputs:
            raise InvalidSpec("correlation inputs must be non-empty, unique, and non-recursive")
        object.__setattr__(self, "inputs", inputs)
        object.__setattr__(
            self,
            "parameters",
            cast(Mapping[str, JsonValue], freeze_json(cast(JsonValue, self.parameters))),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, object], field_path: str) -> CorrelationSpec:
        _keys(
            value,
            required={"field", "kind", "inputs"},
            optional={"parameters"},
            field_path=field_path,
        )
        return cls(
            field=cast(str, value["field"]),
            kind=cast(str, value["kind"]),
            inputs=tuple(
                cast(str, item) for item in _array(value["inputs"], f"{field_path}.inputs")
            ),
            parameters=_json_mapping(value.get("parameters", {}), f"{field_path}.parameters"),
        )

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "field": self.field,
            "kind": self.kind,
            "inputs": list(self.inputs),
            "parameters": thaw_json(cast(JsonValue, self.parameters)),
        }


@dataclass(frozen=True, slots=True)
class CollectionSpec:
    collection_id: str
    resource: ResourceReference
    schema: SchemaDocument
    count: int
    fields: Mapping[str, FieldGeneratorSpec] = dc_field(default_factory=dict)
    depends_on: tuple[str, ...] = ()
    correlations: tuple[CorrelationSpec, ...] = ()
    source_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "collection_id", bounded_token(self.collection_id, "collection id", 256)
        )
        resource = ResourceReference.parse(self.resource, catalog="structured")
        schema = validate_schema(self.schema)
        if (schema.ref.catalog, schema.ref.namespace) != (resource.catalog, resource.namespace):
            raise InvalidSpec("Collection Resource and Schema must share Catalog and Namespace")
        if isinstance(self.count, bool) or not isinstance(self.count, int) or self.count < 1:
            raise InvalidSpec("Collection count must be a positive integer")
        field_map = dict(self.fields)
        unknown = set(field_map) - set(schema.field_map)
        if unknown:
            raise InvalidSpec(
                "Collection generator targets unknown Schema fields",
                details={"fields": ",".join(sorted(unknown))},
            )
        correlations = tuple(self.correlations)
        correlation_fields = {item.field for item in correlations}
        if len(correlation_fields) != len(correlations):
            raise InvalidSpec("Collection correlation targets must be unique")
        for item in correlations:
            missing = ({item.field, *item.inputs}) - set(schema.field_map)
            if missing:
                raise InvalidSpec(
                    "Collection correlation references unknown fields",
                    details={"fields": ",".join(sorted(missing))},
                )
        dependencies = tuple(
            sorted({bounded_token(item, "Collection dependency", 256) for item in self.depends_on})
        )
        if len(dependencies) != len(self.depends_on) or self.collection_id in dependencies:
            raise InvalidSpec("Collection dependencies must be unique and non-recursive")
        if self.source_id is not None:
            object.__setattr__(self, "source_id", bounded_token(self.source_id, "source id", 256))
        object.__setattr__(self, "resource", resource)
        object.__setattr__(self, "schema", schema)
        object.__setattr__(self, "fields", MappingProxyType(dict(sorted(field_map.items()))))
        object.__setattr__(self, "depends_on", dependencies)
        object.__setattr__(self, "correlations", correlations)

    @classmethod
    def from_mapping(cls, value: Mapping[str, object], field_path: str) -> CollectionSpec:
        _keys(
            value,
            required={"id", "resource", "schema", "count"},
            optional={"fields", "dependsOn", "correlations", "source"},
            field_path=field_path,
        )
        resource = ResourceReference.parse(_mapping(value["resource"], f"{field_path}.resource"))
        schema_value = _mapping(value["schema"], f"{field_path}.schema")
        _keys(
            schema_value,
            required={"name", "version", "definition"},
            optional={"fingerprint"},
            field_path=f"{field_path}.schema",
        )
        definition = _mapping(schema_value["definition"], f"{field_path}.schema.definition")
        try:
            schema = SchemaDocument.from_definition(
                catalog=resource.catalog,
                namespace=resource.namespace,
                name=cast(str, schema_value["name"]),
                version=cast(str, schema_value["version"]),
                definition=definition,
            )
            validate_schema(schema)
        except (TypeError, ValueError) as exc:
            raise InvalidSpec(
                "Collection Schema is invalid", field_path=f"{field_path}.schema"
            ) from exc
        declared = schema_value.get("fingerprint")
        if declared is not None and declared != schema.fingerprint:
            raise InvalidSpec(
                "Collection Schema fingerprint does not match canonical definition",
                field_path=f"{field_path}.schema.fingerprint",
            )
        fields_value = _mapping(value.get("fields", {}), f"{field_path}.fields")
        fields = {
            str(name): FieldGeneratorSpec.from_mapping(
                _mapping(item, f"{field_path}.fields.{name}"), f"{field_path}.fields.{name}"
            )
            for name, item in fields_value.items()
        }
        correlations = tuple(
            CorrelationSpec.from_mapping(
                _mapping(item, f"{field_path}.correlations[{index}]"),
                f"{field_path}.correlations[{index}]",
            )
            for index, item in enumerate(
                _array(value.get("correlations", ()), f"{field_path}.correlations")
            )
        )
        return cls(
            collection_id=cast(str, value["id"]),
            resource=resource,
            schema=schema,
            count=_positive(value["count"], f"{field_path}.count"),
            fields=fields,
            depends_on=tuple(
                cast(str, item)
                for item in _array(value.get("dependsOn", ()), f"{field_path}.dependsOn")
            ),
            correlations=correlations,
            source_id=cast(str | None, value.get("source")),
        )

    def to_dict(self) -> dict[str, JsonValue]:
        schema_definition = self.schema.to_dict()
        result: dict[str, JsonValue] = {
            "id": self.collection_id,
            "resource": self.resource.to_dict(),
            "schema": {
                "name": self.schema.ref.name,
                "version": cast(str, self.schema.ref.version),
                "definition": schema_definition,
                "fingerprint": self.schema.fingerprint,
            },
            "count": self.count,
            "fields": {name: item.to_dict() for name, item in self.fields.items()},
            "dependsOn": list(self.depends_on),
            "correlations": [item.to_dict() for item in self.correlations],
        }
        if self.source_id is not None:
            result["source"] = self.source_id
        return result


@dataclass(frozen=True, slots=True)
class RelationSpec:
    collection: CollectionSpec
    source_collection: str
    target_collection: str
    source_field: str
    target_field: str

    def __post_init__(self) -> None:
        for name in ("source_collection", "target_collection", "source_field", "target_field"):
            object.__setattr__(self, name, bounded_token(getattr(self, name), name, 256))
        profile = self.collection.schema.profile
        if not isinstance(profile, RelationProfile):
            raise InvalidSpec("Relation Collection Schema must carry a Relation profile")
        if (profile.source_field, profile.target_field) != (self.source_field, self.target_field):
            raise InvalidSpec("Relation endpoint fields disagree with the Schema profile")

    @classmethod
    def from_mapping(cls, value: Mapping[str, object], field_path: str) -> RelationSpec:
        endpoint_keys = {"sourceCollection", "targetCollection", "sourceField", "targetField"}
        common_keys = {
            "id",
            "resource",
            "schema",
            "count",
            "fields",
            "dependsOn",
            "correlations",
            "source",
        }
        _keys(
            value,
            required={"id", "resource", "schema", "count", *endpoint_keys},
            optional=common_keys - {"id", "resource", "schema", "count"},
            field_path=field_path,
        )
        collection_mapping = {key: item for key, item in value.items() if key in common_keys}
        return cls(
            collection=CollectionSpec.from_mapping(collection_mapping, field_path),
            source_collection=cast(str, value["sourceCollection"]),
            target_collection=cast(str, value["targetCollection"]),
            source_field=cast(str, value["sourceField"]),
            target_field=cast(str, value["targetField"]),
        )

    def to_dict(self) -> dict[str, JsonValue]:
        result = self.collection.to_dict()
        result.update(
            {
                "sourceCollection": self.source_collection,
                "targetCollection": self.target_collection,
                "sourceField": self.source_field,
                "targetField": self.target_field,
            }
        )
        return result


def _query_projection(expression: Expression) -> tuple[str, ...]:
    raw = expression.arguments.get("select")
    if not isinstance(raw, Sequence) or isinstance(raw, str | bytes | bytearray) or not raw:
        raise SourcePolicyViolation("source Query must contain an explicit non-empty select")
    names: list[str] = []
    for item in raw:
        if isinstance(item, str):
            names.append(item)
            continue
        if isinstance(item, Mapping):
            expression_value = item.get("expression", item)
            if isinstance(expression_value, Mapping) and expression_value.get("kind") == "field":
                name = expression_value.get("name")
                if isinstance(name, str):
                    names.append(name)
                    continue
        raise SourcePolicyViolation("source Query projection must contain direct field projections")
    return tuple(names)


@dataclass(frozen=True, slots=True)
class SourceSpec:
    source_id: str
    resource: ResourceReference
    query: Expression
    projection: tuple[str, ...]
    transformation_id: str
    transformation_digest: str
    max_rows: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_id", bounded_token(self.source_id, "source id", 256))
        resource = ResourceReference.parse(self.resource, catalog="structured")
        if self.query.catalog != "structured" or self.query.method != "query":
            raise SourcePolicyViolation("source-backed generation accepts only structured.query")
        raw_resource = self.query.arguments.get("resource")
        try:
            query_resource = ResourceReference.parse(cast(Any, raw_resource), catalog="structured")
        except (TypeError, ValueError) as exc:
            raise SourcePolicyViolation("source Query has an invalid Resource") from exc
        if query_resource != resource:
            raise SourcePolicyViolation("source Query Resource differs from the approved boundary")
        projection = tuple(
            bounded_token(item, "source projection field", 256) for item in self.projection
        )
        if not projection or len(set(projection)) != len(projection):
            raise SourcePolicyViolation("source projection must be non-empty and unique")
        if set(_query_projection(self.query)) != set(projection):
            raise SourcePolicyViolation("source Query select differs from the approved projection")
        object.__setattr__(self, "resource", resource)
        object.__setattr__(self, "projection", projection)
        object.__setattr__(
            self, "transformation_id", bounded_token(self.transformation_id, "transformation id")
        )
        object.__setattr__(
            self,
            "transformation_digest",
            require_digest(self.transformation_digest, "transformation digest"),
        )
        if (
            isinstance(self.max_rows, bool)
            or not isinstance(self.max_rows, int)
            or self.max_rows < 1
        ):
            raise SourcePolicyViolation("source maxRows must be a positive integer")

    @property
    def query_fingerprint(self) -> str:
        return cast(str, self.query.fingerprint)

    @classmethod
    def from_mapping(cls, value: Mapping[str, object], field_path: str) -> SourceSpec:
        _keys(
            value,
            required={
                "id",
                "resource",
                "query",
                "projection",
                "transformation",
                "transformationDigest",
                "maxRows",
            },
            field_path=field_path,
        )
        try:
            query = Expression.from_mapping(_mapping(value["query"], f"{field_path}.query"))
            resource = ResourceReference.parse(
                _mapping(value["resource"], f"{field_path}.resource"), catalog="structured"
            )
        except (TypeError, ValueError) as exc:
            raise SourcePolicyViolation("source Query envelope is invalid") from exc
        return cls(
            source_id=cast(str, value["id"]),
            resource=resource,
            query=query,
            projection=tuple(
                cast(str, item) for item in _array(value["projection"], f"{field_path}.projection")
            ),
            transformation_id=cast(str, value["transformation"]),
            transformation_digest=cast(str, value["transformationDigest"]),
            max_rows=_positive(value["maxRows"], f"{field_path}.maxRows"),
        )

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "id": self.source_id,
            "resource": self.resource.to_dict(),
            "query": self.query.to_dict(),
            "projection": list(self.projection),
            "transformation": self.transformation_id,
            "transformationDigest": self.transformation_digest,
            "maxRows": self.max_rows,
        }


@dataclass(frozen=True, slots=True)
class ValidationRuleSpec:
    rule_id: str
    kind: str
    field_path: str | None = None
    parameters: Mapping[str, JsonValue] = dc_field(default_factory=dict)
    blocking: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "rule_id", bounded_token(self.rule_id, "validation rule id"))
        object.__setattr__(self, "kind", bounded_token(self.kind, "validation rule kind"))
        if self.field_path is not None:
            object.__setattr__(
                self, "field_path", bounded_token(self.field_path, "validation field path")
            )
        object.__setattr__(
            self,
            "parameters",
            cast(Mapping[str, JsonValue], freeze_json(cast(JsonValue, self.parameters))),
        )
        if not isinstance(self.blocking, bool):
            raise InvalidSpec("validation blocking must be boolean")

    @classmethod
    def from_mapping(cls, value: Mapping[str, object], field_path: str) -> ValidationRuleSpec:
        _keys(
            value,
            required={"id", "kind"},
            optional={"fieldPath", "parameters", "blocking"},
            field_path=field_path,
        )
        return cls(
            rule_id=cast(str, value["id"]),
            kind=cast(str, value["kind"]),
            field_path=cast(str | None, value.get("fieldPath")),
            parameters=_json_mapping(value.get("parameters", {}), f"{field_path}.parameters"),
            blocking=cast(bool, value.get("blocking", True)),
        )

    def to_dict(self) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {
            "id": self.rule_id,
            "kind": self.kind,
            "parameters": thaw_json(cast(JsonValue, self.parameters)),
            "blocking": self.blocking,
        }
        if self.field_path is not None:
            result["fieldPath"] = self.field_path
        return result


@dataclass(frozen=True, slots=True)
class SyntheticSpecV1:
    spec_id: str
    version: str
    seed: int | str
    locales: tuple[str, ...]
    time_bounds: TimeBounds
    bounds: ExecutionBounds
    implementation: ImplementationPin
    output_mode: OutputMode
    collections: tuple[CollectionSpec, ...]
    relations: tuple[RelationSpec, ...] = ()
    sources: tuple[SourceSpec, ...] = ()
    validation_rules: tuple[ValidationRuleSpec, ...] = ()
    format_version: str = SPEC_FORMAT_VERSION

    def __post_init__(self) -> None:
        if self.format_version != SPEC_FORMAT_VERSION:
            raise InvalidSpec(f"formatVersion must be {SPEC_FORMAT_VERSION!r}")
        object.__setattr__(self, "spec_id", bounded_token(self.spec_id, "spec id"))
        if not isinstance(self.version, str) or _VERSION_RE.fullmatch(self.version) is None:
            raise InvalidSpec("spec version must use semantic version syntax")
        if isinstance(self.seed, bool) or not isinstance(self.seed, int | str):
            raise InvalidSpec("seed must be an integer or bounded string")
        if isinstance(self.seed, str):
            object.__setattr__(self, "seed", bounded_token(self.seed, "seed"))
        locales = tuple(self.locales)
        if (
            not locales
            or len(set(locales)) != len(locales)
            or any(_LOCALE_RE.fullmatch(item) is None for item in locales)
        ):
            raise InvalidSpec("locales must be non-empty, unique BCP-47-like tags")
        object.__setattr__(self, "locales", locales)
        object.__setattr__(self, "output_mode", OutputMode(self.output_mode))
        collections = tuple(self.collections)
        relations = tuple(self.relations)
        if not collections:
            raise InvalidSpec("spec requires at least one Record Collection")
        identifiers = [item.collection_id for item in collections] + [
            item.collection.collection_id for item in relations
        ]
        if len(set(identifiers)) != len(identifiers):
            raise InvalidSpec("Collection and Relation ids must be globally unique")
        record_ids = {item.collection_id for item in collections}
        source_ids = {item.source_id for item in self.sources}
        if len(source_ids) != len(self.sources):
            raise InvalidSpec("source ids must be unique")
        for item in collections:
            if item.source_id is not None and item.source_id not in source_ids:
                raise InvalidSpec(f"Collection {item.collection_id!r} references an unknown source")
        for relation in relations:
            if (
                relation.source_collection not in record_ids
                or relation.target_collection not in record_ids
            ):
                raise InvalidSpec("Relation endpoints must name Record Collections")
        total = sum(item.count for item in collections) + sum(
            item.collection.count for item in relations
        )
        if total > self.bounds.max_records:
            raise InvalidSpec("declared record count exceeds bounds.maxRecords")
        dependencies = {item.collection_id: item.depends_on for item in collections} | {
            item.collection.collection_id: (
                *item.collection.depends_on,
                item.source_collection,
                item.target_collection,
            )
            for item in relations
        }
        _validate_acyclic(dependencies)
        object.__setattr__(self, "collections", collections)
        object.__setattr__(self, "relations", relations)
        object.__setattr__(self, "sources", tuple(self.sources))
        object.__setattr__(self, "validation_rules", tuple(self.validation_rules))

    @property
    def fingerprint(self) -> str:
        return sha256_digest(cast(JsonValue, self.to_dict()))

    @property
    def schema_fingerprints(self) -> Mapping[str, str]:
        values = {item.collection_id: item.schema.fingerprint for item in self.collections} | {
            item.collection.collection_id: item.collection.schema.fingerprint
            for item in self.relations
        }
        return MappingProxyType(dict(sorted(values.items())))

    @classmethod
    def load(cls, value: bytes | str | Mapping[str, object]) -> SyntheticSpecV1:
        try:
            if isinstance(value, bytes):
                if len(value) > 8 * 1024 * 1024:
                    raise InvalidSpec("spec bytes exceed the 8 MiB parser limit")
                parsed: object = json.loads(value.decode("utf-8"))
            elif isinstance(value, str):
                if len(value.encode("utf-8")) > 8 * 1024 * 1024:
                    raise InvalidSpec("spec text exceeds the 8 MiB parser limit")
                parsed = json.loads(value)
            else:
                parsed = value
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InvalidSpec("spec is not valid UTF-8 JSON") from exc
        root = _mapping(parsed, "$")
        _keys(
            root,
            required={
                "formatVersion",
                "id",
                "version",
                "seed",
                "locales",
                "timeBounds",
                "bounds",
                "implementation",
                "outputMode",
                "collections",
            },
            optional={"relations", "sources", "validation"},
            field_path="$",
        )
        collections = tuple(
            CollectionSpec.from_mapping(
                _mapping(item, f"collections[{index}]"), f"collections[{index}]"
            )
            for index, item in enumerate(_array(root["collections"], "collections"))
        )
        relations = tuple(
            RelationSpec.from_mapping(_mapping(item, f"relations[{index}]"), f"relations[{index}]")
            for index, item in enumerate(_array(root.get("relations", ()), "relations"))
        )
        sources = tuple(
            SourceSpec.from_mapping(_mapping(item, f"sources[{index}]"), f"sources[{index}]")
            for index, item in enumerate(_array(root.get("sources", ()), "sources"))
        )
        validations = tuple(
            ValidationRuleSpec.from_mapping(
                _mapping(item, f"validation[{index}]"), f"validation[{index}]"
            )
            for index, item in enumerate(_array(root.get("validation", ()), "validation"))
        )
        try:
            output_mode = OutputMode(cast(str, root["outputMode"]))
        except ValueError as exc:
            raise InvalidSpec("outputMode must be records, dataset, or streaming") from exc
        return cls(
            spec_id=cast(str, root["id"]),
            version=cast(str, root["version"]),
            seed=cast(int | str, root["seed"]),
            locales=tuple(cast(str, item) for item in _array(root["locales"], "locales")),
            time_bounds=TimeBounds.from_mapping(_mapping(root["timeBounds"], "timeBounds")),
            bounds=ExecutionBounds.from_mapping(_mapping(root["bounds"], "bounds")),
            implementation=ImplementationPin.from_mapping(
                _mapping(root["implementation"], "implementation")
            ),
            output_mode=output_mode,
            collections=collections,
            relations=relations,
            sources=sources,
            validation_rules=validations,
            format_version=cast(str, root["formatVersion"]),
        )

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "formatVersion": self.format_version,
            "id": self.spec_id,
            "version": self.version,
            "seed": self.seed,
            "locales": list(self.locales),
            "timeBounds": self.time_bounds.to_dict(),
            "bounds": self.bounds.to_dict(),
            "implementation": self.implementation.to_dict(),
            "outputMode": self.output_mode.value,
            "collections": [item.to_dict() for item in self.collections],
            "relations": [item.to_dict() for item in self.relations],
            "sources": [item.to_dict() for item in self.sources],
            "validation": [item.to_dict() for item in self.validation_rules],
        }


def _validate_acyclic(dependencies: Mapping[str, Sequence[str]]) -> None:
    names = set(dependencies)
    for name, values in dependencies.items():
        unknown = set(values) - names
        if unknown:
            raise InvalidSpec(
                f"Collection {name!r} has unknown dependencies",
                details={"dependencies": ",".join(sorted(unknown))},
            )
    temporary: set[str] = set()
    permanent: set[str] = set()

    def visit(name: str) -> None:
        if name in permanent:
            return
        if name in temporary:
            raise InvalidSpec("Collection dependency graph contains a cycle")
        temporary.add(name)
        for dependency in dependencies[name]:
            visit(dependency)
        temporary.remove(name)
        permanent.add(name)

    for name in sorted(names):
        visit(name)


SyntheticSpec = SyntheticSpecV1

__all__ = [
    "SPEC_FORMAT_VERSION",
    "CollectionSpec",
    "CorrelationSpec",
    "ExecutionBounds",
    "FieldGeneratorSpec",
    "ImplementationPin",
    "OutputMode",
    "RelationSpec",
    "SourceSpec",
    "SyntheticSpec",
    "SyntheticSpecV1",
    "TimeBounds",
    "ValidationRuleSpec",
]
