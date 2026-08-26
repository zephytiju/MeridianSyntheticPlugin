# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import math

import pytest

from meridian_storage.plugins.synthetic.canonical import (
    bounded_token,
    canonical_json_bytes,
    derive_seed,
    deterministic_uuid,
    freeze_json,
    require_digest,
    sha256_digest,
    thaw_json,
)
from meridian_storage.plugins.synthetic.errors import InvalidSpec, SinkConflict


def test_canonical_json_and_seed_are_stable() -> None:
    left = {"z": [2, 1], "a": {"x": True}}
    right = {"a": {"x": True}, "z": [2, 1]}
    assert canonical_json_bytes(left) == canonical_json_bytes(right)
    assert sha256_digest(left) == sha256_digest(right)
    assert derive_seed(1, "a", 2) == derive_seed(1, "a", 2)
    assert derive_seed(1, "a", 2) != derive_seed(1, "a", 3)
    assert deterministic_uuid(1, "x") == deterministic_uuid(1, "x")


def test_freeze_and_thaw_reject_non_json() -> None:
    frozen = freeze_json({"b": [1, 2]})
    assert thaw_json(frozen) == {"b": [1, 2]}
    with pytest.raises(ValueError):
        freeze_json(math.inf)
    with pytest.raises(TypeError):
        freeze_json(object())  # type: ignore[arg-type]


def test_token_and_digest_validation() -> None:
    digest = sha256_digest({"ok": True})
    assert require_digest(digest, "digest") == digest
    assert bounded_token("valid-token@1", "token") == "valid-token@1"
    with pytest.raises(ValueError):
        require_digest("bad", "digest")
    with pytest.raises(ValueError):
        bounded_token("has spaces", "token")


def test_error_envelope_is_safe_and_stable() -> None:
    error = SinkConflict(
        "write conflict",
        field_path="users.id",
        checkpoint=("users-000",),
        details={"partitionId": "users-000"},
    )
    assert error.retryable
    assert error.to_dict()["code"] == "MERIDIAN_SYNTHETIC_SINK_CONFLICT"
    with pytest.raises(ValueError):
        InvalidSpec("x", details={str(index): "x" for index in range(33)})
