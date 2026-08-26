# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from typing import Any

import pytest

from meridian_storage import Expression, OperationContext
from meridian_storage.evidence import EvidenceCatalogSurface
from meridian_storage.plugins.synthetic import (
    IMPLEMENTATION_COORDINATE,
    CollectionSpec,
    ExecutionBounds,
    FieldGeneratorSpec,
    ImplementationPin,
    OutputMode,
    RelationSpec,
    SyntheticSpec,
    TimeBounds,
)
from meridian_storage.semantics import (
    PROFILE_EXTENSION_KEY,
    Cardinality,
    CatalogName,
    FieldDefinition,
    IndexDefinition,
    LogicalKind,
    LogicalType,
    RelationProfile,
    ResourceReference,
    SchemaDocument,
    SchemaReference,
    SemanticKind,
    StructuredCatalogSurface,
)
from meridian_storage.streaming import StreamingCatalogSurface


def resource(name: str) -> ResourceReference:
    return ResourceReference(CatalogName.STRUCTURED, "fixtures", name)


def basic_schema(name: str = "users") -> SchemaDocument:
    return SchemaDocument(
        SchemaReference(CatalogName.STRUCTURED, "fixtures", name, "1.0.0"),
        SemanticKind.RELATIONAL,
        (
            FieldDefinition("id", LogicalType(LogicalKind.INT64)),
            FieldDefinition(
                "name",
                LogicalType(LogicalKind.STRING),
                constraints={"minLength": 4, "maxLength": 24},
            ),
            FieldDefinition(
                "score",
                LogicalType(LogicalKind.FLOAT64),
                nullable=True,
                constraints={"min": 0, "max": 100},
            ),
            FieldDefinition(
                "tags",
                LogicalType(LogicalKind.STRING),
                cardinality=Cardinality.MANY,
                constraints={"minItems": 1, "maxItems": 3, "uniqueItems": False},
            ),
        ),
        ("id",),
    )


def basic_spec(
    *,
    output_mode: OutputMode = OutputMode.RECORDS,
    count: int = 9,
    batch_size: int = 3,
    max_bytes: int = 2_000_000,
) -> SyntheticSpec:
    collection = CollectionSpec(
        "users",
        resource("users"),
        basic_schema(),
        count,
        fields={
            "name": FieldGeneratorSpec("core.string@1", {"prefix": "user-"}),
            "score": FieldGeneratorSpec("core.float@1", {"min": 0, "max": 100}, null_rate=0.25),
            "tags": FieldGeneratorSpec("core.choice@1", {"values": ["red", "green", "blue"]}),
        },
    )
    return SyntheticSpec(
        "fixture-suite",
        "1.0.0",
        8675309,
        ("en-US", "zh-CN"),
        TimeBounds("2026-01-01T00:00:00Z", "2026-01-31T23:59:59Z"),
        ExecutionBounds(1000, max_bytes, 30_000, 2_000_000, batch_size),
        ImplementationPin(IMPLEMENTATION_COORDINATE),
        output_mode,
        (collection,),
    )


def relation_spec(*, output_mode: OutputMode = OutputMode.RECORDS) -> SyntheticSpec:
    account_schema = SchemaDocument(
        SchemaReference(CatalogName.STRUCTURED, "fixtures", "accounts", "1.0.0"),
        SemanticKind.RELATIONAL,
        (FieldDefinition("id", LogicalType(LogicalKind.INT64)),),
        ("id",),
    )
    order_schema = SchemaDocument(
        SchemaReference(CatalogName.STRUCTURED, "fixtures", "orders", "1.0.0"),
        SemanticKind.RELATIONAL,
        (FieldDefinition("id", LogicalType(LogicalKind.INT64)),),
        ("id",),
    )
    accounts = CollectionSpec("accounts", resource("accounts"), account_schema, 3)
    orders = CollectionSpec("orders", resource("orders"), order_schema, 5)
    profile = RelationProfile(
        "source",
        "target",
        True,
        (resource("accounts"),),
        (resource("orders"),),
    )
    edge_schema = SchemaDocument(
        SchemaReference(CatalogName.STRUCTURED, "fixtures", "ownership", "1.0.0"),
        SemanticKind.RELATION,
        (
            FieldDefinition("id", LogicalType(LogicalKind.INT64)),
            FieldDefinition(
                "source",
                LogicalType(LogicalKind.RECORD_REF),
                constraints={"allowedCollections": (resource("accounts").to_dict(),)},
            ),
            FieldDefinition(
                "target",
                LogicalType(LogicalKind.RECORD_REF),
                constraints={"allowedCollections": (resource("orders").to_dict(),)},
            ),
        ),
        ("id",),
        indexes=(
            IndexDefinition("source_endpoint", "relation-endpoint", ("source",)),
            IndexDefinition("target_endpoint", "relation-endpoint", ("target",)),
        ),
        extensions={PROFILE_EXTENSION_KEY: profile.to_dict()},
    )
    edges = CollectionSpec("ownership", resource("ownership"), edge_schema, 11)
    relation = RelationSpec(edges, "accounts", "orders", "source", "target")
    return SyntheticSpec(
        "relation-suite",
        "1.0.0",
        "relation-seed",
        ("en",),
        TimeBounds("2026-02-01T00:00:00Z", "2026-02-02T00:00:00Z"),
        ExecutionBounds(100, 2_000_000, 30_000, 2_000_000, 4),
        ImplementationPin(IMPLEMENTATION_COORDINATE),
        output_mode,
        (accounts, orders),
        (relation,),
    )


class FakeMeridian:
    def __init__(self, *, query_rows: list[Mapping[str, object]] | None = None) -> None:
        self.expressions: list[Expression] = []
        self.contexts: list[OperationContext] = []
        self.query_rows = query_rows or []
        self.surfaces: dict[str, object] = {
            "structured": StructuredCatalogSurface(),
            "streaming": StreamingCatalogSurface(),
            "evidence": EvidenceCatalogSurface(),
        }

    def catalog(self, name: str) -> object:
        return self.surfaces[name]

    @contextmanager
    def context(self, context: OperationContext):
        self.contexts.append(context)
        yield context

    def execute(self, expression: Expression) -> Any:
        self.expressions.append(expression)
        data: Mapping[str, object] = (
            {"items": self.query_rows}
            if expression.catalog == "structured" and expression.method == "query"
            else {"accepted": True}
        )
        return type("Result", (), {"data": data})()


@pytest.fixture
def operation_context() -> OperationContext:
    return OperationContext(
        "principal:test-suite",
        request_id="request-1",
        tenant="tenant-a",
        scope={"purpose": "synthetic-test"},
    )
