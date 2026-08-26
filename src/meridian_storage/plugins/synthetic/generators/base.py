# SPDX-License-Identifier: Apache-2.0
"""Versioned deterministic generator registry and execution context."""

from __future__ import annotations

import random
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dc_field
from types import MappingProxyType
from typing import Protocol, cast, runtime_checkable

from meridian_storage.semantics import FieldDefinition, FrozenJson, LogicalKind

from ..canonical import (
    JsonValue,
    bounded_token,
    derive_seed,
    freeze_json,
    require_digest,
    sha256_digest,
)
from ..errors import InvalidSpec, NondeterministicImplementation, UnknownGenerator
from ..spec import FieldGeneratorSpec


@dataclass(frozen=True, slots=True)
class GenerationContext:
    spec_seed: int | str
    run_id: str
    collection_id: str
    field: FieldDefinition
    ordinal: int
    element_index: int
    locale: str
    time_start: str
    time_end: str
    values: Mapping[str, FrozenJson] = dc_field(default_factory=dict)
    source: Mapping[str, FrozenJson] = dc_field(default_factory=dict)
    references: Mapping[str, Sequence[Mapping[str, JsonValue]]] = dc_field(default_factory=dict)
    identity_field: bool = False

    def rng(self, stream: str = "value") -> random.Random:
        return random.Random(
            derive_seed(
                self.spec_seed,
                self.collection_id,
                self.ordinal,
                self.field.name,
                self.element_index,
                stream,
            )
        )


@runtime_checkable
class ValueGenerator(Protocol):
    def __call__(self, context: GenerationContext, config: Mapping[str, JsonValue]) -> object: ...


ConfigValidator = Callable[[Mapping[str, JsonValue]], None]
ResourceEstimator = Callable[[Mapping[str, JsonValue]], int]


def _default_estimate(config: Mapping[str, JsonValue]) -> int:
    del config
    return 256


@dataclass(frozen=True, slots=True)
class GeneratorDefinition:
    generator_id: str
    implementation_digest: str
    logical_kinds: frozenset[LogicalKind]
    generate: ValueGenerator
    config_schema: Mapping[str, JsonValue] = dc_field(default_factory=dict)
    validate_config: ConfigValidator = lambda config: None
    estimate_bytes: ResourceEstimator = _default_estimate
    deterministic: bool = True
    locales: frozenset[str] = frozenset({"*"})

    def __post_init__(self) -> None:
        object.__setattr__(self, "generator_id", bounded_token(self.generator_id, "generator id"))
        object.__setattr__(
            self,
            "implementation_digest",
            require_digest(self.implementation_digest, "generator implementation digest"),
        )
        if not self.logical_kinds:
            raise ValueError("generator must support at least one logical kind")
        if not callable(self.generate) or not callable(self.validate_config):
            raise TypeError("generator implementation and validator must be callable")
        object.__setattr__(
            self,
            "config_schema",
            cast(Mapping[str, JsonValue], freeze_json(cast(JsonValue, self.config_schema))),
        )

    def descriptor(self) -> dict[str, JsonValue]:
        return {
            "id": self.generator_id,
            "implementationDigest": self.implementation_digest,
            "logicalKinds": sorted(item.value for item in self.logical_kinds),
            "deterministic": self.deterministic,
            "locales": sorted(self.locales),
            "configSchema": self.config_schema,
        }


class GeneratorRegistry:
    """Mutable-at-composition, immutable-at-execution registry."""

    def __init__(self) -> None:
        self._definitions: dict[str, GeneratorDefinition] = {}
        self._sealed = False

    def register(self, definition: GeneratorDefinition) -> None:
        if self._sealed:
            raise RuntimeError("generator registry is sealed")
        if definition.generator_id in self._definitions:
            raise InvalidSpec(f"duplicate generator id {definition.generator_id!r}")
        self._definitions[definition.generator_id] = definition

    def seal(self) -> GeneratorRegistry:
        self._sealed = True
        return self

    @property
    def implementation_digest(self) -> str:
        return sha256_digest(
            cast(
                JsonValue,
                [self._definitions[name].descriptor() for name in sorted(self._definitions)],
            )
        )

    @property
    def definitions(self) -> Mapping[str, GeneratorDefinition]:
        return MappingProxyType(dict(sorted(self._definitions.items())))

    def resolve(
        self,
        spec: FieldGeneratorSpec,
        field: FieldDefinition,
        locales: Sequence[str],
    ) -> GeneratorDefinition:
        try:
            definition = self._definitions[spec.generator_id]
        except KeyError as exc:
            raise UnknownGenerator(
                f"unknown generator {spec.generator_id!r}", field_path=field.name
            ) from exc
        if not definition.deterministic:
            raise NondeterministicImplementation(
                f"generator {spec.generator_id!r} does not declare deterministic behavior",
                field_path=field.name,
            )
        if (
            spec.expected_digest is not None
            and spec.expected_digest != definition.implementation_digest
        ):
            raise NondeterministicImplementation(
                f"generator {spec.generator_id!r} implementation digest differs from its pin",
                field_path=field.name,
            )
        if field.logical_type.kind not in definition.logical_kinds:
            raise InvalidSpec(
                f"generator {spec.generator_id!r} does not support {field.logical_type.kind.value}",
                field_path=field.name,
            )
        unsupported = set(locales) - set(definition.locales)
        if "*" not in definition.locales and unsupported:
            raise InvalidSpec(
                f"generator {spec.generator_id!r} does not support all requested locales",
                field_path=field.name,
                details={"locales": ",".join(sorted(unsupported))},
            )
        try:
            definition.validate_config(spec.config)
        except (TypeError, ValueError) as exc:
            raise InvalidSpec(
                f"generator {spec.generator_id!r} configuration is invalid",
                field_path=field.name,
            ) from exc
        return definition


def exact_config(
    *,
    required: frozenset[str] = frozenset(),
    optional: frozenset[str] = frozenset(),
) -> ConfigValidator:
    """Create a strict top-level configuration validator."""

    def validate(config: Mapping[str, JsonValue]) -> None:
        missing = set(required) - set(config)
        unknown = set(config) - set(required) - set(optional)
        if missing or unknown:
            raise ValueError(
                f"generator config has missing={sorted(missing)!r}, unknown={sorted(unknown)!r}"
            )

    return validate


__all__ = [
    "ConfigValidator",
    "GenerationContext",
    "GeneratorDefinition",
    "GeneratorRegistry",
    "ResourceEstimator",
    "ValueGenerator",
    "exact_config",
]
