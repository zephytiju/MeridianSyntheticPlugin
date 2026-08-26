# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest

from meridian_storage.plugins.synthetic import (
    FieldGeneratorSpec,
    GeneratorDefinition,
    GeneratorRegistry,
    InvalidSpec,
    NondeterministicImplementation,
    UnknownGenerator,
    builtin_registry,
)
from meridian_storage.plugins.synthetic.canonical import sha256_digest
from meridian_storage.semantics import FieldDefinition, LogicalKind, LogicalType


def test_builtin_registry_covers_all_logical_kinds() -> None:
    registry = builtin_registry()
    covered = set().union(*(item.logical_kinds for item in registry.definitions.values()))
    assert covered == set(LogicalKind)
    assert registry.implementation_digest.startswith("sha256:")
    with pytest.raises(RuntimeError, match="sealed"):
        registry.register(next(iter(registry.definitions.values())))


def test_registry_rejects_duplicates_unknown_and_wrong_kind() -> None:
    definition = GeneratorDefinition(
        "test.value@1",
        sha256_digest({"generator": "test"}),
        frozenset({LogicalKind.STRING}),
        lambda context, config: "value",
    )
    registry = GeneratorRegistry()
    registry.register(definition)
    with pytest.raises(InvalidSpec, match="duplicate"):
        registry.register(definition)
    field = FieldDefinition("value", LogicalType(LogicalKind.STRING))
    with pytest.raises(UnknownGenerator):
        registry.resolve(FieldGeneratorSpec("missing@1"), field, ("en",))
    number = FieldDefinition("value", LogicalType(LogicalKind.INT64))
    with pytest.raises(InvalidSpec, match="does not support"):
        registry.resolve(FieldGeneratorSpec("test.value@1"), number, ("en",))


def test_registry_enforces_determinism_and_digest_pins() -> None:
    digest = sha256_digest({"generator": "test"})
    registry = GeneratorRegistry()
    registry.register(
        GeneratorDefinition(
            "test.value@1",
            digest,
            frozenset({LogicalKind.STRING}),
            lambda context, config: "value",
            deterministic=False,
        )
    )
    field = FieldDefinition("value", LogicalType(LogicalKind.STRING))
    with pytest.raises(NondeterministicImplementation):
        registry.resolve(FieldGeneratorSpec("test.value@1"), field, ("en",))

    pinned = GeneratorRegistry()
    pinned.register(
        GeneratorDefinition(
            "test.value@1",
            digest,
            frozenset({LogicalKind.STRING}),
            lambda context, config: "value",
        )
    )
    with pytest.raises(NondeterministicImplementation, match="pin"):
        pinned.resolve(
            FieldGeneratorSpec("test.value@1", expected_digest="sha256:" + "f" * 64),
            field,
            ("en",),
        )
