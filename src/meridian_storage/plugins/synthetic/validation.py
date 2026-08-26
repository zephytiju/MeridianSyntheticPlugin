# SPDX-License-Identifier: Apache-2.0
"""Incremental schema, identity, relation, and domain validation."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from meridian_storage.semantics import FrozenJson, Record

from .canonical import JsonValue, canonical_json_bytes, sha256_digest, thaw_json
from .errors import InvalidSpec
from .spec import ValidationRuleSpec


class ValidationStatus(StrEnum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


@dataclass(frozen=True, slots=True)
class RuleResult:
    rule_id: str
    status: ValidationStatus
    observed: JsonValue
    expected: JsonValue
    blocking: bool

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "ruleId": self.rule_id,
            "status": self.status.value,
            "observed": self.observed,
            "expected": self.expected,
            "blocking": self.blocking,
        }


@dataclass(frozen=True, slots=True)
class ValidationReport:
    results: tuple[RuleResult, ...]

    @property
    def passed(self) -> bool:
        return not any(
            item.status is ValidationStatus.FAIL and item.blocking for item in self.results
        )

    @property
    def digest(self) -> str:
        return sha256_digest(cast(JsonValue, [item.to_dict() for item in self.results]))

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "passed": self.passed,
            "digest": self.digest,
            "results": [item.to_dict() for item in self.results],
        }


def _field_target(path: str | None) -> tuple[str, str]:
    if path is None or path.count(".") != 1:
        raise InvalidSpec("validation fieldPath must be '<collection>.<field>'")
    collection_id, field_name = path.split(".", 1)
    if not collection_id or not field_name:
        raise InvalidSpec("validation fieldPath must name a Collection and field")
    return collection_id, field_name


def _scalar_key(value: object) -> bytes:
    return canonical_json_bytes(thaw_json(cast(JsonValue, value)))


class ValidationAccumulator:
    """Collect bounded summaries while partitions are generated and written."""

    _SUPPORTED = frozenset({"count", "unique", "null-rate", "range", "allowed-values"})

    def __init__(
        self,
        *,
        expected_counts: Mapping[str, int],
        identity_fields: Mapping[str, Sequence[str]],
        rules: Sequence[ValidationRuleSpec],
        reserve_memory: Callable[[int], None] | None = None,
    ) -> None:
        unknown = {item.kind for item in rules} - self._SUPPORTED
        if unknown:
            raise InvalidSpec(
                "unsupported validation rule kind",
                details={"kinds": ",".join(sorted(unknown))},
            )
        self._expected = dict(expected_counts)
        self._identity_fields = {name: tuple(fields) for name, fields in identity_fields.items()}
        self._rules = tuple(rules)
        self._reserve_memory = reserve_memory or (lambda amount: None)
        self._tracked_fields = {
            _field_target(item.field_path) for item in rules if item.kind != "count"
        }
        self._counts: defaultdict[str, int] = defaultdict(int)
        self._identities: defaultdict[str, set[bytes]] = defaultdict(set)
        self._values: defaultdict[tuple[str, str], list[FrozenJson]] = defaultdict(list)
        self._record_refs: list[tuple[str, bytes]] = []

    def observe(self, collection_id: str, record: Record) -> None:
        self._counts[collection_id] += 1
        identity = _scalar_key(record.record_id)
        if identity not in self._identities[collection_id]:
            self._reserve_memory(len(identity) + 64)
            self._identities[collection_id].add(identity)
        for field_name, value in record.values.items():
            if (collection_id, field_name) in self._tracked_fields:
                self._reserve_memory(len(canonical_json_bytes(cast(JsonValue, value))) + 64)
                self._values[(collection_id, field_name)].append(value)
            self._collect_references(value)

    def _collect_references(self, value: FrozenJson) -> None:
        if isinstance(value, Mapping):
            collection = value.get("collectionRef")
            if isinstance(collection, Mapping) and "recordId" in value:
                canonical = (
                    f"{collection.get('catalog')}:{collection.get('namespace')}."
                    f"{collection.get('name')}"
                )
                self._reserve_memory(
                    len(canonical.encode("utf-8")) + len(_scalar_key(value["recordId"])) + 64
                )
                self._record_refs.append((canonical, _scalar_key(value["recordId"])))
            return
        if isinstance(value, tuple):
            for item in value:
                self._collect_references(item)

    def finish(
        self,
        *,
        reference_identities: Mapping[str, frozenset[bytes]],
    ) -> ValidationReport:
        results: list[RuleResult] = []
        for collection_id in sorted(self._expected):
            expected = self._expected[collection_id]
            observed = self._counts[collection_id]
            results.append(
                RuleResult(
                    f"builtin.count.{collection_id}",
                    ValidationStatus.PASS if observed == expected else ValidationStatus.FAIL,
                    observed,
                    expected,
                    True,
                )
            )
            unique = len(self._identities[collection_id])
            results.append(
                RuleResult(
                    f"builtin.identity.{collection_id}",
                    ValidationStatus.PASS if unique == observed else ValidationStatus.FAIL,
                    unique,
                    observed,
                    True,
                )
            )
        invalid_refs = sum(
            1
            for collection, identity in self._record_refs
            if identity not in reference_identities.get(collection, frozenset())
        )
        results.append(
            RuleResult(
                "builtin.relation-integrity",
                ValidationStatus.PASS if invalid_refs == 0 else ValidationStatus.FAIL,
                invalid_refs,
                0,
                True,
            )
        )
        results.extend(self._evaluate_rule(item) for item in self._rules)
        return ValidationReport(tuple(results))

    def _evaluate_rule(self, rule: ValidationRuleSpec) -> RuleResult:
        if rule.kind == "count":
            collection = rule.field_path
            if collection is None or "." in collection:
                raise InvalidSpec("count validation fieldPath must be a Collection id")
            observed = self._counts[collection]
            expected = rule.parameters.get("equals")
            if isinstance(expected, bool) or not isinstance(expected, int):
                raise InvalidSpec("count validation requires integer parameters.equals")
            passed = observed == expected
            return self._result(rule, observed, expected, passed)

        collection, field_name = _field_target(rule.field_path)
        values = self._values[(collection, field_name)]
        if rule.kind == "unique":
            non_null = [item for item in values if item is not None]
            observed = len({_scalar_key(item) for item in non_null})
            expected = len(non_null)
            return self._result(rule, observed, expected, observed == expected)
        if rule.kind == "null-rate":
            null_rate = 0.0 if not values else sum(item is None for item in values) / len(values)
            maximum = rule.parameters.get("max", 0.0)
            if isinstance(maximum, bool) or not isinstance(maximum, int | float):
                raise InvalidSpec("null-rate validation parameters.max must be numeric")
            expected = float(maximum)
            return self._result(rule, null_rate, expected, null_rate <= expected)
        if rule.kind == "range":
            lower = rule.parameters.get("min")
            upper = rule.parameters.get("max")
            numeric = [float(item) for item in values if isinstance(item, int | float)]
            if not numeric or any(not math.isfinite(item) for item in numeric):
                return self._result(rule, len(numeric), len(values), False)
            passed = (lower is None or min(numeric) >= float(cast(int | float, lower))) and (
                upper is None or max(numeric) <= float(cast(int | float, upper))
            )
            observed_range: JsonValue = {"min": min(numeric), "max": max(numeric)}
            return self._result(rule, observed_range, dict(rule.parameters), passed)

        allowed = rule.parameters.get("values")
        if not isinstance(allowed, Sequence) or isinstance(allowed, str | bytes | bytearray):
            raise InvalidSpec("allowed-values validation requires parameters.values")
        allowed_keys = {_scalar_key(item) for item in allowed}
        invalid = sum(_scalar_key(item) not in allowed_keys for item in values)
        return self._result(rule, invalid, 0, invalid == 0)

    @staticmethod
    def _result(
        rule: ValidationRuleSpec,
        observed: JsonValue,
        expected: JsonValue,
        passed: bool,
    ) -> RuleResult:
        status = (
            ValidationStatus.PASS
            if passed
            else ValidationStatus.FAIL
            if rule.blocking
            else ValidationStatus.WARN
        )
        return RuleResult(rule.rule_id, status, observed, expected, rule.blocking)


__all__ = [
    "RuleResult",
    "ValidationAccumulator",
    "ValidationReport",
    "ValidationStatus",
]
