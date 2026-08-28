# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from conftest import FakeMeridian, basic_spec

from meridian_storage import Meridian
from meridian_storage.plugins.synthetic import (
    MemorySink,
    Synthetic,
    SyntheticPluginFactory,
    SyntheticSchemaProvider,
)
from meridian_storage.runtime import (
    CatalogsConfig,
    LiveSchemaConfig,
    ResourcesConfig,
    RetryPolicy,
    RuntimeConfig,
    SchemaConfig,
    ValidationConfig,
)
from meridian_storage.spi import PluginFactory, SchemaProvider


def test_plugin_factory_implements_core_v1_contract() -> None:
    factory = SyntheticPluginFactory()
    assert isinstance(factory, PluginFactory)
    assert factory.plugin_id == "synthetic"
    manifest = factory.manifest()
    assert manifest.plugin_id == factory.plugin_id
    assert manifest.plugin_version == "1.0.1"
    assert manifest.plugin_contract_version == "1.0.0"
    assert manifest.core_contract == "1.x"
    assert manifest.extensions["distribution"] == "meridian-storage-plugin-synthetic"
    assert manifest.extensions["service"] == "false"


def test_plugin_facade_preserves_direct_generator_contract() -> None:
    runtime = FakeMeridian()
    plugin = SyntheticPluginFactory().create(runtime)  # type: ignore[arg-type]
    assert isinstance(plugin, Synthetic)
    spec = basic_spec(count=2, batch_size=1)
    run = plugin.generator(spec).run(MemorySink())
    assert run.progress.generated_records == 2
    assert plugin.repository.resources.specs.catalog == "structured"


def test_schema_provider_contributes_only_synthetic_logical_resources() -> None:
    provider = SyntheticSchemaProvider()
    assert isinstance(provider, SchemaProvider)
    bundle = provider.load()
    assert bundle.provider_id == "synthetic"
    assert bundle.provider_version == "1.0.1"
    assert bundle.provider_contract_version == "1.0.0"
    assert {(item.catalog, item.name) for item in bundle.namespaces} == {
        ("structured", "synthetic")
    }
    assert {item.ref.name for item in bundle.resources} == {"specs", "run-evidence"}
    assert bundle.extensions["distribution"] == "meridian-storage-plugin-synthetic"
    assert bundle.extensions["catalogsOwned"] == ()


def test_core_runtime_discovers_and_loads_plugin_in_process() -> None:
    config = RuntimeConfig(
        profile="synthetic-plugin-test",
        catalogs=CatalogsConfig(()),
        resources=ResourcesConfig(()),
        schemas=SchemaConfig((), LiveSchemaConfig(False, False, None)),
        bindings=(),
        placements=(),
        validation=ValidationConfig(True, False, 1_000, 16, RetryPolicy(1, 1, 1, 0.0)),
    )
    runtime = Meridian(config)
    report = runtime.start()
    try:
        assert "synthetic" in report.plugins
        assert isinstance(runtime.plugin("synthetic"), Synthetic)
    finally:
        runtime.close()
