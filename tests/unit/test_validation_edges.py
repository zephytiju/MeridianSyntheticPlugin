# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from conftest import basic_spec

from meridian_storage.plugins.synthetic import (
    Generator,
    InvalidSpec,
    MemorySink,
    SyntheticSpec,
    ValidationFailure,
    ValidationRuleSpec,
)


def _with_rules(*rules: ValidationRuleSpec) -> SyntheticSpec:
    spec = basic_spec(count=8)
    return SyntheticSpec(
        spec.spec_id,
        spec.version,
        spec.seed,
        spec.locales,
        spec.time_bounds,
        spec.bounds,
        spec.implementation,
        spec.output_mode,
        spec.collections,
        validation_rules=rules,
    )


def test_null_rate_and_allowed_values_validation() -> None:
    spec = _with_rules(
        ValidationRuleSpec("nulls", "null-rate", "users.score", {"max": 1}),
        ValidationRuleSpec(
            "tags",
            "allowed-values",
            "users.tags",
            {
                "values": [
                    ["red"],
                    ["green"],
                    ["blue"],
                    ["red", "green"],
                    ["red", "blue"],
                    ["green", "blue"],
                    ["red", "green", "blue"],
                ]
            },
            blocking=False,
        ),
    )
    run = Generator(spec).run(MemorySink())
    assert run.validation.results[-2].status.value == "pass"
    assert run.validation.results[-1].status.value in {"pass", "warn"}


@pytest.mark.parametrize(
    "rule",
    [
        ValidationRuleSpec("unknown", "not-a-rule", "users.name"),
        ValidationRuleSpec("bad-count-path", "count", "users.id", {"equals": 1}),
        ValidationRuleSpec("bad-count", "count", "users", {"equals": "one"}),
        ValidationRuleSpec("bad-null", "null-rate", "users.score", {"max": "all"}),
        ValidationRuleSpec("bad-allowed", "allowed-values", "users.name", {"values": "all"}),
    ],
)
def test_invalid_validation_configuration(rule: ValidationRuleSpec) -> None:
    with pytest.raises(InvalidSpec):
        Generator(_with_rules(rule)).run(MemorySink())


def test_non_numeric_range_fails() -> None:
    with pytest.raises(ValidationFailure):
        Generator(
            _with_rules(ValidationRuleSpec("name-range", "range", "users.name", {"min": 1}))
        ).run(MemorySink())
