# SPDX-License-Identifier: Apache-2.0
"""Stable, safe synthetic-plugin error model."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import StrEnum
from types import MappingProxyType

_MAX_MESSAGE_BYTES = 4096
_MAX_DETAILS = 32
_MAX_DETAIL_KEY_BYTES = 128
_MAX_DETAIL_VALUE_BYTES = 512


class SyntheticErrorCode(StrEnum):
    INVALID_SPEC = "MERIDIAN_SYNTHETIC_INVALID_SPEC"
    UNKNOWN_GENERATOR = "MERIDIAN_SYNTHETIC_UNKNOWN_GENERATOR"
    NONDETERMINISTIC_IMPLEMENTATION = "MERIDIAN_SYNTHETIC_NONDETERMINISTIC_IMPLEMENTATION"
    EXCEEDED_BUDGET = "MERIDIAN_SYNTHETIC_EXCEEDED_BUDGET"
    CANCELLED = "MERIDIAN_SYNTHETIC_CANCELLED"
    SINK_CONFLICT = "MERIDIAN_SYNTHETIC_SINK_CONFLICT"
    VALIDATION_FAILED = "MERIDIAN_SYNTHETIC_VALIDATION_FAILED"
    PUBLICATION_FAILED = "MERIDIAN_SYNTHETIC_PUBLICATION_FAILED"
    SOURCE_POLICY = "MERIDIAN_SYNTHETIC_SOURCE_POLICY"


class SyntheticError(Exception):
    """Base error containing only bounded, non-sensitive diagnostic metadata."""

    code: SyntheticErrorCode = SyntheticErrorCode.INVALID_SPEC
    retryable = False

    def __init__(
        self,
        message: str,
        *,
        field_path: str | None = None,
        checkpoint: Sequence[str] = (),
        details: Mapping[str, str] | None = None,
    ) -> None:
        if (
            not isinstance(message, str)
            or not message
            or len(message.encode()) > _MAX_MESSAGE_BYTES
        ):
            raise ValueError("synthetic error messages must be bounded")
        self.message = message
        self.field_path = field_path
        self.checkpoint = tuple(checkpoint)
        safe_details = dict(details or {})
        if len(safe_details) > _MAX_DETAILS or any(
            not isinstance(key, str)
            or not isinstance(value, str)
            or len(key.encode()) > _MAX_DETAIL_KEY_BYTES
            or len(value.encode()) > _MAX_DETAIL_VALUE_BYTES
            for key, value in safe_details.items()
        ):
            raise ValueError("synthetic error details must be bounded strings")
        self.details = MappingProxyType(dict(sorted(safe_details.items())))
        super().__init__(message)

    def to_dict(self) -> dict[str, object]:
        return {
            "code": self.code.value,
            "message": self.message,
            "fieldPath": self.field_path,
            "checkpoint": list(self.checkpoint),
            "details": dict(self.details),
            "retryable": self.retryable,
        }


class InvalidSpec(SyntheticError):
    code = SyntheticErrorCode.INVALID_SPEC


class UnknownGenerator(SyntheticError):
    code = SyntheticErrorCode.UNKNOWN_GENERATOR


class NondeterministicImplementation(SyntheticError):
    code = SyntheticErrorCode.NONDETERMINISTIC_IMPLEMENTATION


class ExceededBudget(SyntheticError):
    code = SyntheticErrorCode.EXCEEDED_BUDGET


class CancelledRun(SyntheticError):
    code = SyntheticErrorCode.CANCELLED


class SinkConflict(SyntheticError):
    code = SyntheticErrorCode.SINK_CONFLICT
    retryable = True


class ValidationFailure(SyntheticError):
    code = SyntheticErrorCode.VALIDATION_FAILED


class PublicationFailure(SyntheticError):
    code = SyntheticErrorCode.PUBLICATION_FAILED
    retryable = True


class SourcePolicyViolation(SyntheticError):
    code = SyntheticErrorCode.SOURCE_POLICY


__all__ = [
    "CancelledRun",
    "ExceededBudget",
    "InvalidSpec",
    "NondeterministicImplementation",
    "PublicationFailure",
    "SinkConflict",
    "SourcePolicyViolation",
    "SyntheticError",
    "SyntheticErrorCode",
    "UnknownGenerator",
    "ValidationFailure",
]
