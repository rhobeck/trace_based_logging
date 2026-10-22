import os
import socket
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.trace_based_logging import extraction, storage
from src.trace_based_logging.config import (build_node_url, validate_config,
                                            initialize_extraction_state)
from src.trace_based_logging.raw_trace_retriever import trace_retriever_utils


def node_available():
    try:
        with socket.create_connection(("127.0.0.1", 8081), timeout=2):
            return True
    except OSError:
        return False


def events_only_config(**overrides):
    """Flat config for a node-only (events-source) discovery run on the verified
    1-log window of the Augur main contract (chain-checked: exactly 1 log there)."""
    config = {
        "protocol": "http://", "host": "127.0.0.1", "port": 8081,
        "contracts_dapp": ["0x75228dce4d82566d93068a8d5d49435216551599"],
        "contracts_non_dapp": ["0x0000000000000000000000000000000000000000"],
        "min_block": 5926229, "max_block": 5928228,
        "extract_normal_transactions": False,
        "extract_internal_transactions": False,
        "extract_transactions_by_events": True,
        "etherscan_api_key": "ETHERSCAN_API_KEY",
        "dapp_events": True, "dapp_calls": True, "dapp_zero_value_calls": True,
        "dapp_delegatecalls": True, "dapp_creations": True,
        "non_dapp_events": True, "non_dapp_calls": True,
        "non_dapp_zero_value_calls": True, "non_dapp_delegatecalls": True,
        "non_dapp_creations": True,
        "log_folder": "output", "sensitive_events": False,
        "extraction": True, "decoding": False, "transformation": False,
        "ordering": "paper",
    }
    config.update(overrides)
    return config


# ------------------------------- config validation -------------------------------

def test_validate_config_events_only_needs_no_etherscan_key():
    validate_config(events_only_config())      # placeholder key, must not raise


def test_validate_config_requires_key_for_etherscan_sources():
    with pytest.raises(ValueError):
        validate_config(events_only_config(extract_normal_transactions=True))


def test_validate_config_rejects_unknown_ordering():
    with pytest.raises(ValueError):
        validate_config(events_only_config(ordering="bogus"))


def test_creation_blocks_earliest_create_per_frontier_contract():
    tree = pd.DataFrame({
        "type": ["CALL", "CREATE", "CREATE2", "CREATE", "CREATE"],
        "to": ["0xAAA", "0xAAA", "0xbbb", "0xaaa", "0xccc"],
        "blockNumber": [50, 300, 200, 100, 400],
    })
    # earliest CREATE (reverted ones included), case-insensitive; CALLs and
    # contracts outside the frontier ignored; untraced creations absent
    assert extraction.creation_blocks(tree, {"0xaaa", "0xbbb", "0xddd"}) == \
        {"0xaaa": 100, "0xbbb": 200}
    assert extraction.creation_blocks(None, {"0xaaa"}) == {}


def _install_fake_sources(monkeypatch, fail):
    """Fake discovery sources: Etherscan rows as strings, event rows as ints (as the
    real sources type them); one hash found by two sources (dedup); internal can
    be made to fail. Returns the per-source call counter."""
    calls = {"normal": 0, "internal": 0, "events": 0}

    def fake_etherscan(contracts, lo, hi, internal_flag, etherscan_api_key, chainid=1):
        calls[internal_flag] += 1
        if internal_flag == "internal" and fail["internal"]:
            raise RuntimeError("Etherscan error (simulated query timeout)")
        tag = "n" if internal_flag == "normal" else "i"
        return pd.DataFrame([{"hash": f"0x{tag}{k}", "blockNumber": str(lo + k),
                              "timeStamp": str(1000 + k)} for k in range(3)])

    def fake_events(node_url, contracts, lo, hi, start_blocks=None):
        calls["events"] += 1
        return pd.DataFrame({"blockNumber": [lo + 1, lo + 7], "timeStamp": [1001, 1007],
                             "hash": ["0xn1", "0xe7"], "contractAddress": ["0xA", "0xA"]})

    monkeypatch.setattr(extraction.get_transactions, "get_transactions", fake_etherscan)
    monkeypatch.setattr(extraction.get_transactions, "get_transactions_by_events",
                        fake_events)
    return calls


def test_fetch_transactions_persists_sources_and_resumes(tmp_path, monkeypatch):
    """A failure in the internal source keeps the finished normal source on disk;
    the rerun reuses it and yields exactly the uninterrupted result."""
    fail = {"internal": False}
    calls = _install_fake_sources(monkeypatch, fail)
    config = events_only_config(extract_normal_transactions=True,
                                extract_internal_transactions=True)
    reference = extraction.fetch_transactions(config, ["0xabc"])       # no persistence
    assert not os.listdir(tmp_path)

    prefix = str(tmp_path / "transactions_level-03")
    fail["internal"] = True
    with pytest.raises(RuntimeError):
        extraction.fetch_transactions(config, ["0xabc"], level_prefix=prefix)
    assert sorted(os.listdir(tmp_path)) == ["transactions_level-03.normal.parquet"]

    fail["internal"] = False
    resumed = extraction.fetch_transactions(config, ["0xabc"], level_prefix=prefix)
    pd.testing.assert_frame_equal(resumed, reference)
    assert calls == {"normal": 2, "internal": 3, "events": 2}     # normal not refetched
    assert storage.parquet_block_range(f"{prefix}.events.parquet") == \
        (config["min_block"], config["max_block"])

    again = extraction.fetch_transactions(config, ["0xabc"], level_prefix=prefix)
    pd.testing.assert_frame_equal(again, reference)
    assert calls == {"normal": 2, "internal": 3, "events": 2}     # everything reused


def test_saved_source_is_logged(tmp_path, monkeypatch, caplog):
    import logging
    _install_fake_sources(monkeypatch, {"internal": False})
    prefix = str(tmp_path / "transactions_level-03")
    with caplog.at_level(logging.INFO):
        extraction.fetch_transactions(events_only_config(), ["0xabc"], level_prefix=prefix)
    assert (f"EVENTS transactions: 2 rows saved to {prefix}.events.parquet"
            in [r.getMessage() for r in caplog.records])


def test_source_file_from_other_block_range_is_refused(tmp_path, monkeypatch):
    _install_fake_sources(monkeypatch, {"internal": False})
    prefix = str(tmp_path / "transactions_level-01")
    extraction.fetch_transactions(events_only_config(max_block=5929999), ["0xabc"],
                                  level_prefix=prefix)
    with pytest.raises(RuntimeError, match="new log_folder"):
        extraction.fetch_transactions(events_only_config(), ["0xabc"], level_prefix=prefix)


def _frontier_state(**overrides):
    state = {"level": 1, "base_contract": "0xabc", "contracts_dapp": {"0xabc"},
             "contracts_lx": {"0xabc"}, "contracts_non_dapp": set(),
             "all_transactions": set()}
    state.update(overrides)
    return state


def test_checkpoint_from_other_block_range_is_refused(tmp_path):
    config = events_only_config()
    storage.save_checkpoint(str(tmp_path), _frontier_state(), block_range=(1, 2))
    state = initialize_extraction_state(config, trace_retriever_utils)
    with pytest.raises(RuntimeError, match="new log_folder"):
        extraction.process_transactions(config, state, extraction_dir=str(tmp_path))


def test_checkpoint_without_block_range_is_accepted(tmp_path, caplog):
    """Checkpoints written before ranges were recorded (e.g. a run in progress)
    still resume, with a warning."""
    import logging
    config = events_only_config()
    storage.save_checkpoint(str(tmp_path), _frontier_state(contracts_lx=set(), level=4))
    state = initialize_extraction_state(config, trace_retriever_utils)
    with caplog.at_level(logging.WARNING):
        extraction.process_transactions(config, state, extraction_dir=str(tmp_path))
    assert state["level"] == 4
    assert any("records no block range" in r.getMessage() for r in caplog.records)


def test_level_list_from_other_block_range_is_refused(tmp_path):
    config = events_only_config()
    storage.save_checkpoint(str(tmp_path), _frontier_state(),
                            block_range=(config["min_block"], config["max_block"]))
    storage.write_atomic_parquet(str(tmp_path / "transactions_level-01.parquet"),
                                 pd.DataFrame({"hash": ["0x1"]}), block_range=(1, 2))
    state = initialize_extraction_state(config, trace_retriever_utils)
    with pytest.raises(RuntimeError, match="new log_folder"):
        extraction.process_transactions(config, state, extraction_dir=str(tmp_path))


def test_fetch_transactions_all_sources_disabled_is_empty():
    config = events_only_config(extract_transactions_by_events=False)
    df = extraction.fetch_transactions(config, ["0xabc"])
    assert df.empty


# ------------------------------- live discovery run -------------------------------

@pytest.mark.skipif(not node_available(), reason="archive node at 127.0.0.1:8081 not reachable")
def test_process_transactions_events_only_with_persistence(tmp_path):
    config = events_only_config()
    state = initialize_extraction_state(config, trace_retriever_utils)
    extraction_dir = str(tmp_path / "extraction")

    extraction.process_transactions(config, state, extraction_dir=extraction_dir)

    # one transaction discovered in the verified window; its trace extracted
    assert state["trace_tree"] is not None and len(state["trace_tree"]) > 0
    assert state["all_transactions"]
    assert {"hash", "tracePos", "tracePosDepth", "orderExact", "type",
            "timeStamp", "blockNumber"}.issubset(state["trace_tree"].columns)
    # per-transaction enumeration is gapless (paper ordering mode)
    for _, grp in state["trace_tree"].groupby("hash"):
        assert sorted(grp["tracePos"]) == list(range(1, len(grp) + 1))

    # persistence: level transaction list, trace parts, checkpoint
    assert os.path.exists(os.path.join(extraction_dir, "transactions_level-01.parquet"))
    assert storage.completed_parts(extraction_dir, "trace_tree/level-01")
    checkpoint = storage.load_checkpoint(extraction_dir)
    assert checkpoint is not None
    assert checkpoint["all_transactions"] == state["all_transactions"]
    assert checkpoint["contracts_dapp"] == state["contracts_dapp"]

    # parts on disk equal the in-memory level data
    level1 = storage.read_dataset(extraction_dir, "trace_tree/level-01")
    assert len(level1) == len(state["trace_tree"][
        state["trace_tree"]["hash"].isin(pd.read_parquet(
            os.path.join(extraction_dir, "transactions_level-01.parquet"))["hash"])])

    # resume: a fresh state picks up the checkpoint and finds nothing new to do
    state2 = initialize_extraction_state(config, trace_retriever_utils)
    extraction.process_transactions(config, state2, extraction_dir=extraction_dir)
    assert state2["contracts_dapp"] == state["contracts_dapp"]
    assert state2["all_transactions"] == state["all_transactions"]


@pytest.mark.skipif(not node_available(), reason="archive node at 127.0.0.1:8081 not reachable")
def test_insert_transaction_index_and_save_roundtrip(tmp_path):
    from src.trace_based_logging.saving import folder_set_up, save_trace_data
    from src.trace_based_logging.trace_decoder import data_preparation

    config = events_only_config()
    state = initialize_extraction_state(config, trace_retriever_utils)
    extraction_dir = str(tmp_path / "extraction")
    extraction.process_transactions(config, state, extraction_dir=extraction_dir)

    cache = str(tmp_path / "receipts_cache.parquet")
    extraction.insert_transaction_index(config, state, build_node_url,
                                        cache_path=cache, extraction_dir=extraction_dir)
    assert state["trace_tree"]["transactionIndex"].notna().all()
    assert str(state["trace_tree"]["transactionIndex"].dtype) == "int64"
    assert os.path.exists(os.path.join(extraction_dir, "receipts.parquet"))
    assert {"txHash", "transactionIndex", "status", "logCount"}.issubset(
        state["receipts"].columns)

    # save under the established naming scheme, then reload through load_data
    dir_path = str(tmp_path / "project")
    folder_set_up(dir_path, config)
    save_trace_data(config, state, dir_path)

    class _Cd:  # load_data derives the project root from its own file location,
        pass    # so point it at our tmp project via monkey layout instead:
    # simpler: read the files directly under the same naming scheme
    suffix = f"{state['base_contract']}_{config['min_block']}_{config['max_block']}"
    out_dir = os.path.join(dir_path, "resources", config["log_folder"], "extraction")
    reloaded = pd.read_parquet(os.path.join(out_dir, f"df_trace_tree_{suffix}.parquet"))
    assert len(reloaded) == len(state["trace_tree"])
    contracts = pd.read_parquet(os.path.join(out_dir, f"contracts_dapp_{suffix}.parquet"))
    assert set(contracts["address"]) == state["contracts_dapp"]
    with open(os.path.join(out_dir, f"contracts_dapp_{suffix}.txt")) as fh:
        assert set(fh.read().splitlines()) == state["contracts_dapp"]
