import os
import sys

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.trace_based_logging import storage


def test_part_roundtrip_and_completed_parts(tmp_path):
    base = str(tmp_path)
    df1 = pd.DataFrame({"hash": ["0xa", "0xb"], "tracePos": [1, 2]})
    df2 = pd.DataFrame({"hash": ["0xc"], "tracePos": [1]})
    storage.write_part(base, "trace_tree/level-01", df1, 1)
    storage.write_part(base, "trace_tree/level-01", df2, 2)

    assert storage.completed_parts(base, "trace_tree/level-01") == [1, 2]
    combined = storage.read_dataset(base, "trace_tree/level-01")
    assert len(combined) == 3
    assert list(storage.read_part(base, "trace_tree/level-01", 2)["hash"]) == ["0xc"]
    # column projection works
    assert list(storage.read_dataset(base, "trace_tree/level-01", columns=["hash"]).columns) == ["hash"]
    # atomic writes leave no temp files behind
    d = os.path.join(base, "trace_tree/level-01")
    assert not [f for f in os.listdir(d) if f.endswith(".tmp")]


def test_read_missing_dataset_is_empty(tmp_path):
    assert storage.read_dataset(str(tmp_path), "nothing").empty
    assert storage.completed_parts(str(tmp_path), "nothing") == []


def test_reset_dataset(tmp_path):
    base = str(tmp_path)
    storage.write_part(base, "ds", pd.DataFrame({"x": [1]}), 1)
    storage.reset_dataset(base, "ds")
    assert storage.completed_parts(base, "ds") == []


def test_checkpoint_roundtrip(tmp_path):
    base = str(tmp_path)
    state = {
        "level": 3,
        "base_contract": "0xroot",
        "contracts_dapp": {"0xroot", "0xchild"},
        "contracts_lx": {"0xchild"},
        "contracts_non_dapp": {"0x" + "0" * 40},
        "all_transactions": {"0xt1", "0xt2", "0xt3"},
    }
    storage.save_checkpoint(base, state)
    loaded = storage.load_checkpoint(base)
    assert loaded["level"] == 3
    assert loaded["base_contract"] == "0xroot"
    assert loaded["contracts_dapp"] == state["contracts_dapp"]
    assert loaded["contracts_lx"] == state["contracts_lx"]
    assert loaded["all_transactions"] == state["all_transactions"]
    # hash set lives in the parquet sidecar, not the JSON
    json_size = os.path.getsize(os.path.join(base, storage.CHECKPOINT_JSON))
    assert json_size < 1000
    assert os.path.exists(os.path.join(base, storage.CHECKPOINT_TXS))


def test_load_checkpoint_absent(tmp_path):
    assert storage.load_checkpoint(str(tmp_path)) is None


def test_checkpoint_records_block_range(tmp_path):
    base = str(tmp_path)
    state = {"level": 2, "base_contract": "0xroot", "contracts_dapp": {"0xroot"},
             "contracts_lx": set(), "contracts_non_dapp": set(), "all_transactions": set()}
    storage.save_checkpoint(base, state)
    assert "block_range" not in storage.load_checkpoint(base)      # older form
    storage.save_checkpoint(base, state, block_range=(5926229, 11229573))
    assert storage.load_checkpoint(base)["block_range"] == [5926229, 11229573]


def test_parquet_records_block_range_without_changing_data(tmp_path):
    df = pd.DataFrame({"hash": ["0x1", "0x2"], "blockNumber": [10, 11],
                       "note": ["a", None]})
    with_range = str(tmp_path / "with.parquet")
    without = str(tmp_path / "without.parquet")
    storage.write_atomic_parquet(with_range, df, block_range=(1, 99))
    storage.write_atomic_parquet(without, df)
    assert storage.parquet_block_range(with_range) == (1, 99)
    assert storage.parquet_block_range(without) is None
    pd.testing.assert_frame_equal(pd.read_parquet(with_range), pd.read_parquet(without))
