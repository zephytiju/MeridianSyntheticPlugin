# SPDX-License-Identifier: Apache-2.0
"""Canonical JSON, fingerprints, and hierarchical deterministic seeds."""

from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import cast

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | Mapping[str, "JsonValue"] | Sequence["JsonValue"]

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,511}$")


def thaw_json(value: JsonValue) -> JsonValue:
    """Return mutable JSON containers while rejecting non-finite numbers."""

    if isinstance(value, Mapping):
        return {str(key): thaw_json(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [thaw_json(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("JSON numbers must be finite")
    return cast(JsonScalar, value)


def freeze_json(value: JsonValue) -> JsonValue:
    """Return recursively immutable, lexically ordered JSON containers."""

    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): freeze_json(item) for key, item in sorted(value.items())}
        )
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return tuple(freeze_json(item) for item in value)
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("JSON numbers must be finite")
    if value is not None and not isinstance(value, str | int | float | bool):
        raise TypeError(f"unsupported JSON value: {type(value).__name__}")
    return value


def canonical_json_bytes(value: JsonValue) -> bytes:
    return json.dumps(
        thaw_json(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_digest(value: JsonValue | bytes) -> str:
    payload = value if isinstance(value, bytes) else canonical_json_bytes(value)
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def require_digest(value: str, field_name: str) -> str:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase sha256 fingerprint")
    return value


def bounded_token(value: str, field_name: str, maximum: int = 512) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > maximum
        or _TOKEN_RE.fullmatch(value) is None
    ):
        raise ValueError(f"{field_name} must be a bounded token")
    return value


def derive_seed(seed: int | str, *parts: object) -> int:
    """Derive an independent random stream from stable logical coordinates."""

    material = canonical_json_bytes(cast(JsonValue, [str(seed), *[str(part) for part in parts]]))
    return int.from_bytes(hashlib.sha256(material).digest()[:16], "big")


def deterministic_uuid(seed: int | str, *parts: object) -> str:
    raw = bytearray(hashlib.sha256(str(derive_seed(seed, *parts)).encode()).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x50
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


_SOURCE_EVIDENCE_KEYS = frozenset(
    {
        "sourceId",
        "queryFingerprint",
        "sourceBoundary",
        "transformationId",
        "transformationDigest",
        "rowCount",
        "rowDigest",
    }
)


def freeze_source_evidence(
    values: Sequence[Mapping[str, JsonValue]],
) -> tuple[Mapping[str, JsonValue], ...]:
    """Validate the metadata-only source evidence envelope and freeze it."""

    result: list[Mapping[str, JsonValue]] = []
    for value in values:
        if set(value) != _SOURCE_EVIDENCE_KEYS:
            raise ValueError("source evidence contains unknown or missing metadata")
        for name in ("sourceId", "sourceBoundary", "transformationId"):
            raw = value[name]
            if not isinstance(raw, str) or not raw:
                raise ValueError(f"source evidence {name} must be a non-empty string")
        for name in ("queryFingerprint", "transformationDigest", "rowDigest"):
            require_digest(cast(str, value[name]), f"source evidence {name}")
        row_count = value["rowCount"]
        if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 1:
            raise ValueError("source evidence rowCount must be a positive integer")
        frozen = freeze_json(cast(JsonValue, value))
        result.append(cast(Mapping[str, JsonValue], frozen))
    return tuple(result)


__all__ = [
    "JsonScalar",
    "JsonValue",
    "bounded_token",
    "canonical_json_bytes",
    "derive_seed",
    "deterministic_uuid",
    "freeze_json",
    "freeze_source_evidence",
    "require_digest",
    "sha256_digest",
    "thaw_json",
]
