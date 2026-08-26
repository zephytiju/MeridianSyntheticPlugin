# SPDX-License-Identifier: Apache-2.0
"""Small entry-point factory for embedding the synthetic generator."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from ._version import __version__
from .canonical import JsonValue
from .execution import IMPLEMENTATION_COORDINATE, Generator
from .generators import GeneratorRegistry
from .spec import SyntheticSpec


@dataclass(frozen=True, slots=True)
class SyntheticPluginManifest:
    package: str = "meridian-plugin-synthetic"
    version: str = __version__
    coordinate: str = IMPLEMENTATION_COORDINATE
    service: bool = False
    catalogs_owned: tuple[str, ...] = ()
    catalogs_used: tuple[str, ...] = ("structured", "evidence", "streaming")

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "package": self.package,
            "version": self.version,
            "coordinate": self.coordinate,
            "service": self.service,
            "catalogsOwned": list(self.catalogs_owned),
            "catalogsUsed": list(self.catalogs_used),
        }


class SyntheticPluginFactory:
    """Entry point that creates embeddable Generator instances, never a service."""

    manifest = SyntheticPluginManifest()

    def __call__(
        self,
        spec: SyntheticSpec | bytes | str | Mapping[str, object],
        *,
        registry: GeneratorRegistry | None = None,
    ) -> Generator:
        return Generator(spec, registry=registry)

    def create(
        self,
        spec: SyntheticSpec | bytes | str | Mapping[str, object],
        *,
        registry: GeneratorRegistry | None = None,
    ) -> Generator:
        return self(spec, registry=registry)


def plugin_manifest() -> Mapping[str, JsonValue]:
    return cast(Mapping[str, JsonValue], SyntheticPluginFactory.manifest.to_dict())


__all__ = ["SyntheticPluginFactory", "SyntheticPluginManifest", "plugin_manifest"]
