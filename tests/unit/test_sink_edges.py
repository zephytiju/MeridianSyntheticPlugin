# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from conftest import basic_spec

from meridian_storage.plugins.synthetic import (
    Generator,
    InMemoryCheckpointStore,
    MemorySink,
)
from meridian_storage.plugins.synthetic.sinks import PartitionReceipt


def test_memory_sink_bounds_records_and_abort() -> None:
    with pytest.raises(ValueError, match="positive"):
        MemorySink(max_bytes=0)
    with pytest.raises(MemoryError, match="limit"):
        Generator(basic_spec(count=2)).run(MemorySink(max_bytes=1))

    sink = MemorySink()
    run = Generator(basic_spec(count=2)).run(sink)
    assert len(sink.records(run.run_id, "users")) == 2
    assert sink.finalize(run.run_id, run.receipts, {})["kind"] == "memory"
    sink.abort(run.run_id, run.receipts, "test")


def test_checkpoint_store_detects_digest_conflict() -> None:
    store = InMemoryCheckpointStore()
    first = PartitionReceipt("users", "part", 1, 10, "sha256:" + "0" * 64)
    second = PartitionReceipt("users", "part", 1, 10, "sha256:" + "1" * 64)
    store.mark_completed("run", first)
    assert store.completed("run") == {"part"}
    with pytest.raises(ValueError, match="conflict"):
        store.mark_completed("run", second)


@pytest.mark.parametrize(
    "receipt",
    [
        ("", "part", 1, 1, "sha256:" + "0" * 64, False, {}),
        ("users", "part", 0, 1, "sha256:" + "0" * 64, False, {}),
        ("users", "part", 1, 1, "bad", False, {}),
        ("users", "part", 1, 1, "sha256:" + "0" * 64, "no", {}),
        ("users", "part", 1, 1, "sha256:" + "0" * 64, False, {"key": 1}),
    ],
)
def test_partition_receipt_rejects_invalid_metadata(receipt) -> None:
    with pytest.raises((TypeError, ValueError)):
        PartitionReceipt(*receipt)
