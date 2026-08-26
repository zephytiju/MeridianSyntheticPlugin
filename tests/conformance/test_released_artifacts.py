# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from importlib.metadata import version

from meridian_storage.plugins.synthetic import SyntheticPluginFactory, plugin_manifest


def test_only_released_v1_meridian_contracts_are_consumed() -> None:
    assert version("meridian-storage-core") == "1.0.0"
    assert version("meridian-storage-semantics") == "1.0.0"
    assert version("meridian-storage-query") == "1.0.0"
    assert version("meridian-storage-evidence") == "1.0.0"
    assert version("meridian-storage-streaming") == "1.0.0"


def test_plugin_manifest_declares_no_catalog_or_service() -> None:
    manifest = plugin_manifest()
    assert manifest["package"] == "meridian-plugin-synthetic"
    assert manifest["service"] is False
    assert manifest["catalogsOwned"] == []
    assert SyntheticPluginFactory.manifest.catalogs_used == (
        "structured",
        "evidence",
        "streaming",
    )
