# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from pathlib import Path

from meridian_storage.plugins.synthetic import (
    IMPLEMENTATION_COORDINATE,
    CollectionSpec,
    ExecutionBounds,
    Generator,
    ImplementationPin,
    MemorySink,
    OutputMode,
    SyntheticSpec,
    TimeBounds,
)
from meridian_storage.semantics import (
    CatalogName,
    FieldDefinition,
    LogicalKind,
    LogicalType,
    ResourceReference,
    SchemaDocument,
    SchemaReference,
    SemanticKind,
)


def test_default_generators_produce_schema_valid_every_logical_type() -> None:
    parent_ref = ResourceReference(CatalogName.STRUCTURED, "fixtures", "parents")
    parent_schema = SchemaDocument(
        SchemaReference(CatalogName.STRUCTURED, "fixtures", "parents", "1.0.0"),
        SemanticKind.RELATIONAL,
        (FieldDefinition("id", LogicalType(LogicalKind.INT64)),),
        ("id",),
    )
    parent = CollectionSpec("parents", parent_ref, parent_schema, 3)
    kinds_ref = ResourceReference(CatalogName.STRUCTURED, "fixtures", "logical-kinds")
    fields = (
        FieldDefinition("id", LogicalType(LogicalKind.INT64)),
        FieldDefinition("boolean_value", LogicalType(LogicalKind.BOOLEAN)),
        FieldDefinition("int8_value", LogicalType(LogicalKind.INT8)),
        FieldDefinition("int16_value", LogicalType(LogicalKind.INT16)),
        FieldDefinition("int32_value", LogicalType(LogicalKind.INT32)),
        FieldDefinition("decimal_value", LogicalType(LogicalKind.DECIMAL, 12, 2)),
        FieldDefinition("float_value", LogicalType(LogicalKind.FLOAT64)),
        FieldDefinition("string_value", LogicalType(LogicalKind.STRING)),
        FieldDefinition("bytes_value", LogicalType(LogicalKind.BYTES)),
        FieldDefinition("uuid_value", LogicalType(LogicalKind.UUID)),
        FieldDefinition("timestamp_value", LogicalType(LogicalKind.UTC_TIMESTAMP)),
        FieldDefinition("date_value", LogicalType(LogicalKind.DATE)),
        FieldDefinition("duration_value", LogicalType(LogicalKind.DURATION)),
        FieldDefinition("enum_value", LogicalType(LogicalKind.ENUM, enum_values=("a", "b", "c"))),
        FieldDefinition("json_value", LogicalType(LogicalKind.JSON)),
        FieldDefinition(
            "record_ref_value",
            LogicalType(LogicalKind.RECORD_REF),
            constraints={"allowedCollections": (parent_ref.to_dict(),)},
        ),
        FieldDefinition("object_ref_value", LogicalType(LogicalKind.OBJECT_REF)),
        FieldDefinition("point_value", LogicalType(LogicalKind.WGS84_POINT)),
    )
    kinds_schema = SchemaDocument(
        SchemaReference(CatalogName.STRUCTURED, "fixtures", "logical-kinds", "1.0.0"),
        SemanticKind.RELATIONAL,
        fields,
        ("id",),
    )
    kinds = CollectionSpec("logical-kinds", kinds_ref, kinds_schema, 6, depends_on=("parents",))
    spec = SyntheticSpec(
        "logical-kind-conformance",
        "1.0.0",
        99,
        ("en",),
        TimeBounds("2026-03-01T00:00:00Z", "2026-03-02T00:00:00Z"),
        ExecutionBounds(100, 1_000_000, 30_000, 1_000_000, 2),
        ImplementationPin(IMPLEMENTATION_COORDINATE),
        OutputMode.RECORDS,
        (parent, kinds),
    )
    sink = MemorySink()
    run = Generator(spec).run(sink, workers=3)
    assert run.validation.passed
    records = sink.records(run.run_id, "logical-kinds")
    assert len(records) == 6
    assert set(records[0].values) == {item.name for item in fields}


def test_package_does_not_import_forbidden_provider_sdks() -> None:
    root = Path(__file__).parents[2]
    source = "\n".join(path.read_text() for path in (root / "src").rglob("*.py"))
    forbidden = ("import kafka", "from kafka", "boto3", "psycopg", "Adapter(", "Engine(")
    assert not any(token in source for token in forbidden)
    metadata = (root / "pyproject.toml").read_text()
    assert "kafka" not in metadata.lower()
    assert "boto3" not in metadata.lower()
