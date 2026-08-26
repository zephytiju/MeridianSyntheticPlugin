# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from conftest import basic_spec
from hypothesis import given, settings
from hypothesis import strategies as st

from meridian_storage.plugins.synthetic import Generator, MemorySink


@given(seed=st.integers(min_value=-(2**63), max_value=2**63 - 1))
@settings(max_examples=20, deadline=None)
def test_equal_seed_is_reproducible_and_different_seed_changes_values(seed: int) -> None:
    spec = basic_spec(count=3)
    seeded = type(spec)(
        spec.spec_id,
        spec.version,
        seed,
        spec.locales,
        spec.time_bounds,
        spec.bounds,
        spec.implementation,
        spec.output_mode,
        spec.collections,
    )
    left_sink = MemorySink()
    right_sink = MemorySink()
    left = Generator(seeded).run(left_sink, run_id="left", workers=1)
    right = Generator(seeded).run(right_sink, run_id="right", workers=3)
    assert [item.to_dict() for item in left_sink.records(left.run_id)] == [
        item.to_dict() for item in right_sink.records(right.run_id)
    ]
