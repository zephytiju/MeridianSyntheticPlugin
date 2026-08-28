# SPDX-License-Identifier: Apache-2.0
"""Meridian V1 plugin factory and in-process Synthetic composition facade."""

from __future__ import annotations

from collections.abc import Mapping

from meridian_storage import Meridian
from meridian_storage.spi import PluginManifest

from ._version import __version__
from .execution import Generator
from .generators import GeneratorRegistry
from .repository import SyntheticRepository, SyntheticResources
from .spec import SyntheticSpec


class Synthetic:
    """In-process facade returned by ``Meridian.plugin("synthetic")``."""

    def __init__(
        self,
        meridian: Meridian,
        *,
        resources: SyntheticResources | None = None,
        registry: GeneratorRegistry | None = None,
    ) -> None:
        if not callable(getattr(meridian, "execute", None)):
            raise TypeError("meridian must implement Meridian.execute(Expression)")
        self._registry = registry
        self.repository = SyntheticRepository(meridian, resources)

    def generator(
        self,
        spec: SyntheticSpec | bytes | str | Mapping[str, object],
        *,
        registry: GeneratorRegistry | None = None,
    ) -> Generator:
        return Generator(spec, registry=registry or self._registry)


class SyntheticPluginFactory:
    """Core-discoverable factory for the embeddable Synthetic plugin."""

    @property
    def plugin_id(self) -> str:
        return "synthetic"

    def manifest(self) -> PluginManifest:
        return PluginManifest(
            plugin_id=self.plugin_id,
            plugin_version=__version__,
            plugin_contract_version="1.0.0",
            core_contract="1.x",
            extensions={
                "distribution": "meridian-storage-plugin-synthetic",
                "catalogs": "structured,evidence,streaming",
                "service": "false",
                "registryService": "false",
                "design.hldRevision": "109",
                "design.syntheticLldRevision": "34",
            },
        )

    def create(self, meridian: Meridian) -> Synthetic:
        return Synthetic(meridian)


def plugin_manifest() -> PluginManifest:
    """Return the exact manifest Core validates during discovery."""

    return SyntheticPluginFactory().manifest()


__all__ = ["Synthetic", "SyntheticPluginFactory", "plugin_manifest"]
