# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from importlib.metadata import entry_points, version

from meridian_storage.plugins.synthetic import (
    SyntheticPluginFactory,
    SyntheticSchemaProvider,
    plugin_manifest,
)


def test_only_released_v1_meridian_contracts_are_consumed() -> None:
    assert version("meridian-storage-core") == "1.0.0"
    assert version("meridian-storage-semantics") == "1.0.0"
    assert version("meridian-storage-query") == "1.0.0"
    assert version("meridian-storage-evidence") == "1.0.0"
    assert version("meridian-storage-streaming") == "1.0.0"
    assert version("meridian-storage-plugin-synthetic") == "1.0.1"


def test_plugin_manifest_declares_no_catalog_or_service() -> None:
    manifest = plugin_manifest()
    assert manifest == SyntheticPluginFactory().manifest()
    assert manifest.extensions["distribution"] == "meridian-storage-plugin-synthetic"
    assert manifest.extensions["service"] == "false"


def test_installed_entry_points_load_the_core_contracts() -> None:
    plugins = {item.name: item for item in entry_points().select(group="meridian_storage.plugins")}
    schemas = {item.name: item for item in entry_points().select(group="meridian_storage.schemas")}
    assert plugins["synthetic"].load() is SyntheticPluginFactory
    assert schemas["synthetic"].load() is SyntheticSchemaProvider
