# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import replace

import pytest

from meridian_storage.plugins.synthetic import (
    FieldGeneratorSpec,
    GenerationContext,
    InvalidSpec,
    builtin_registry,
)
from meridian_storage.semantics import (
    Cardinality,
    CatalogName,
    FieldDefinition,
    LogicalKind,
    LogicalType,
    ResourceReference,
)


def _context(
    kind: LogicalKind,
    *,
    constraints=None,
    identity: bool = False,
    source=None,
    references=None,
) -> GenerationContext:
    logical = (
        LogicalType(kind, 8, 2)
        if kind is LogicalKind.DECIMAL
        else LogicalType(kind, enum_values=("a", "b"))
        if kind is LogicalKind.ENUM
        else LogicalType(kind)
    )
    return GenerationContext(
        spec_seed=7,
        run_id="run",
        collection_id="items",
        field=FieldDefinition(
            "value", logical, cardinality=Cardinality.ONE, constraints=constraints or {}
        ),
        ordinal=2,
        element_index=0,
        locale="zh-CN",
        time_start="2026-01-01T00:00:00Z",
        time_end="2026-01-02T00:00:00Z",
        source=source or {},
        references=references or {},
        identity_field=identity,
    )


def _generate(generator_id: str, context: GenerationContext, config=None):
    registry = builtin_registry()
    spec = FieldGeneratorSpec(generator_id, config or {})
    definition = registry.resolve(spec, context.field, (context.locale,))
    return definition.generate(context, spec.config)


def test_special_generators_and_options() -> None:
    assert _generate("core.constant@1", _context(LogicalKind.STRING), {"value": "fixed"}) == "fixed"
    assert (
        _generate("core.sequence@1", _context(LogicalKind.DECIMAL), {"start": 1, "step": 0.5})
        == "2.00"
    )
    normal = _generate(
        "core.float@1",
        _context(LogicalKind.FLOAT64, constraints={"min": 0, "max": 10}),
        {"distribution": "normal", "mean": 5, "deviation": 1},
    )
    assert 0 <= normal <= 10
    assert _generate("core.boolean@1", _context(LogicalKind.BOOLEAN), {"probability": 1})
    assert (
        _generate(
            "core.choice@1",
            _context(LogicalKind.STRING),
            {"values": ["a", "b"], "weights": [0, 1]},
        )
        == "b"
    )
    assert _generate("core.json@1", _context(LogicalKind.JSON), {"template": {"fixed": True}}) == {
        "fixed": True
    }
    assert _generate("core.localized-name@1", _context(LogicalKind.STRING)) in {
        "晨曦",
        "嘉宁",
        "思远",
        "雨桐",
        "子墨",
        "若溪",
    }
    custom_object = _generate(
        "core.object-ref@1",
        _context(LogicalKind.OBJECT_REF),
        {"resource": "object:fixtures.assets"},
    )
    assert custom_object["resourceRef"]["catalog"] == "object"


@pytest.mark.parametrize(
    ("generator_id", "kind", "config", "match"),
    [
        ("core.integer@1", LogicalKind.INT64, {"min": 3, "max": 2}, "minimum"),
        ("core.float@1", LogicalKind.FLOAT64, {"distribution": "triangle"}, "distribution"),
        ("core.decimal@1", LogicalKind.DECIMAL, {"min": "2", "max": "1"}, "minimum"),
        ("core.boolean@1", LogicalKind.BOOLEAN, {"probability": 2}, "probability"),
        ("core.string@1", LogicalKind.STRING, {"minLength": 4, "maxLength": 2}, "length"),
        ("core.choice@1", LogicalKind.STRING, {"values": []}, "non-empty"),
        ("core.choice@1", LogicalKind.STRING, {"values": ["a"], "weights": []}, "weights"),
        ("core.duration@1", LogicalKind.DURATION, {"minSeconds": -1}, "duration"),
        ("core.bytes@1", LogicalKind.BYTES, {"length": -1}, "byte length"),
        (
            "core.wgs84@1",
            LogicalKind.WGS84_POINT,
            {"minLongitude": 10, "maxLongitude": -10},
            "WGS84",
        ),
        ("core.object-ref@1", LogicalKind.OBJECT_REF, {"resource": 4}, "ObjectRef"),
        ("core.source-field@1", LogicalKind.STRING, {"field": "missing"}, "source-field"),
    ],
)
def test_invalid_generator_values_fail_boundedly(generator_id, kind, config, match) -> None:
    with pytest.raises((TypeError, ValueError), match=match):
        _generate(generator_id, _context(kind), config)


def test_identity_and_reference_edge_cases() -> None:
    with pytest.raises(InvalidSpec, match="cannot represent"):
        _generate(
            "core.integer@1",
            replace(_context(LogicalKind.INT8, identity=True), ordinal=2),
            {"min": 127, "max": 127},
        )
    with pytest.raises(ValueError, match="cannot fit"):
        _generate(
            "core.string@1",
            _context(LogicalKind.STRING, identity=True),
            {"maxLength": 3},
        )
    parent = ResourceReference(CatalogName.STRUCTURED, "fixtures", "parents")
    reference = {"collectionRef": parent.to_dict(), "recordId": 1}
    context = _context(
        LogicalKind.RECORD_REF,
        constraints={"allowedCollections": (parent.to_dict(),)},
        references={parent.canonical: (reference,)},
    )
    assert _generate("core.record-ref@1", context) == reference
    with pytest.raises(ValueError, match="populated"):
        _generate("core.record-ref@1", replace(context, references={}))


def test_registry_configuration_validation_is_strict() -> None:
    context = _context(LogicalKind.STRING)
    registry = builtin_registry()
    with pytest.raises(InvalidSpec, match="configuration"):
        registry.resolve(
            FieldGeneratorSpec("core.string@1", {"unknown": True}),
            context.field,
            ("zh-CN",),
        )
