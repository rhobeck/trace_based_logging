import os
import pickle
import socket
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.trace_based_logging.raw_trace_retriever import trace_transformation as tt
from src.trace_based_logging.raw_trace_retriever import trace_retriever_utils

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
NODE_URL = "http://127.0.0.1:8081"


def node_available():
    try:
        with socket.create_connection(("127.0.0.1", 8081), timeout=2):
            return True
    except OSError:
        return False


def mock_trace_with_position():
    """Root emits log A, calls c1 (which emits log C), then emits log B.

    True execution order: root, A, c1, C, B.
    Paper (Listing-2) enumeration: root, c1, C, A, B.
    """
    return {
        "type": "CALL", "from": "0xroot", "to": "0xcallee",
        "calls": [
            {"type": "CALL", "from": "0xcallee", "to": "0xc1",
             "logs": [{"topics": ["C"], "position": "0x0", "index": "0x1"}]},
        ],
        "logs": [
            {"topics": ["A"], "position": "0x0", "index": "0x0"},
            {"topics": ["B"], "position": "0x1", "index": "0x2"},
        ],
    }


def mock_trace_without_position():
    """A trace as older clients emit it: logs carry no 'position' field."""
    return {
        "CALL_1": "attribute", "calls": [
            {"CALL_1.1": "attribute"},
            {"CALL_1.2": "attribute", "calls": [
                {"CALL_1.2.1": "attribute"},
                {"CALL_1.2.2": "attribute"},
                {"CALL_1.2.3": "attribute", "calls": [
                    {"CALL_1.2.3.1": "attribute"},
                    {"CALL_1.2.3.2": "attribute"},
                ],
                 "logs": [{"topics": ["1.2.3"]}, {"topics": ["1.2.3"]}],
                 },
            ]},
            {"CALL_1.3": "attribute"},
        ],
        "logs": [{"topics": ["1"]}, {"topics": ["1"]}],
    }


# --------------------- default (paper) ordering: the artifact mode ---------------------

def test_default_is_paper_ordering_even_when_position_present():
    """With no ordering argument, the enumeration must be the paper's order
    (Listing 2: calls before a frame's logs), even though the trace carries
    'position' fields that would permit the exact order."""
    trace = mock_trace_with_position()
    tt.insert_tracePos(trace)
    assert trace["tracePos"] == 1
    assert trace["calls"][0]["tracePos"] == 2            # sub-call first ...
    assert trace["calls"][0]["logs"][0]["tracePos"] == 3
    assert trace["logs"][0]["tracePos"] == 4             # ... frame's own logs after
    assert trace["logs"][1]["tracePos"] == 5
    # the approximation is marked: root emits logs AND calls -> children not exact
    assert trace["logs"][0]["orderExact"] is False
    assert trace["calls"][0]["orderExact"] is False

    trace2 = mock_trace_with_position()
    tt.insert_tracePosDepth(trace2)
    assert trace2["calls"][0]["tracePosDepth"] == "1.1"
    assert trace2["calls"][0]["logs"][0]["tracePosDepth"] == "1.1.1"
    assert trace2["logs"][0]["tracePosDepth"] == "1.2"
    assert trace2["logs"][1]["tracePosDepth"] == "1.3"


def test_default_reproduces_original_pipeline_numbering():
    """Regression pin: the default mode must equal the paper-era output exactly
    (assertions lifted from the original test_pytest.py)."""
    trace = mock_trace_without_position()
    tt.insert_tracePos(trace)
    assert trace["tracePos"] == 1
    assert trace["calls"][0]["tracePos"] == 2
    assert trace["calls"][1]["tracePos"] == 3
    assert trace["calls"][1]["calls"][0]["tracePos"] == 4
    assert trace["calls"][1]["calls"][1]["tracePos"] == 5
    assert trace["calls"][1]["calls"][2]["tracePos"] == 6
    assert trace["calls"][1]["calls"][2]["calls"][0]["tracePos"] == 7
    assert trace["calls"][1]["calls"][2]["calls"][1]["tracePos"] == 8
    assert trace["calls"][1]["calls"][2]["logs"][0]["tracePos"] == 9
    assert trace["calls"][1]["calls"][2]["logs"][1]["tracePos"] == 10
    assert trace["calls"][2]["tracePos"] == 11
    assert trace["calls"][1]["calls"][2]["logs"][0]["orderExact"] is False

    trace2 = mock_trace_without_position()
    tt.insert_tracePosDepth(trace2)
    assert trace2["calls"][1]["calls"][2]["tracePosDepth"] == "1.2.3"
    assert trace2["calls"][1]["calls"][2]["logs"][0]["tracePosDepth"] == "1.2.3.3"
    assert trace2["calls"][1]["calls"][2]["logs"][1]["tracePosDepth"] == "1.2.3.4"


def test_default_works_without_position_fields():
    """Paper mode must not require 'position' (paper-era traces lack it)."""
    tt.insert_tracePos(mock_trace_without_position())           # must not raise
    tt.insert_tracePosDepth(mock_trace_without_position())


def test_golden_erigon2_trace_operation_count():
    """Stored paper-era Erigon trace: default mode must emit one record per operation —
    count validated against the raw JSON key counts (== 571 for this transaction)."""
    path = os.path.join(REPO, "tests", "test_resources",
                        "trace_json_lx_0x39a7a29cd1b941424774e0ffa8cc93bcd968f30e3d3d1ee3d7d086916697dc29_erigon2.pkl")
    trace = pickle.load(open(path, "rb"))
    n_frames = trace_retriever_utils.count_string_occurrences_in_keys(trace, "type")
    n_logs = trace_retriever_utils.count_string_occurrences_in_keys(trace, "topics")
    tt.insert_tracePos(trace)
    tt.insert_tracePosDepth(trace)
    records = tt.flatten(trace)
    assert len(records) == n_frames + n_logs
    trace_positions = [r["tracePos"] for r in records]
    assert trace_positions == sorted(trace_positions)
    assert len(set(trace_positions)) == len(records)


# --------------------- exact ordering: validation-suite mode only ---------------------

def test_exact_mode_interleaves_logs_among_calls():
    trace = mock_trace_with_position()
    tt.insert_tracePos(trace, ordering=tt.ORDERING_EXACT)
    assert trace["tracePos"] == 1
    assert trace["logs"][0]["tracePos"] == 2          # log A before the sub-call
    assert trace["calls"][0]["tracePos"] == 3
    assert trace["calls"][0]["logs"][0]["tracePos"] == 4
    assert trace["logs"][1]["tracePos"] == 5          # log B after the sub-call
    assert all(n.get("orderExact", True) for n in
               [trace, trace["calls"][0], trace["logs"][0], trace["logs"][1]])

    trace2 = mock_trace_with_position()
    tt.insert_tracePosDepth(trace2, ordering=tt.ORDERING_EXACT)
    assert trace2["logs"][0]["tracePosDepth"] == "1.1"
    assert trace2["calls"][0]["tracePosDepth"] == "1.2"
    assert trace2["calls"][0]["logs"][0]["tracePosDepth"] == "1.2.1"
    assert trace2["logs"][1]["tracePosDepth"] == "1.3"


def test_exact_mode_requires_position():
    with pytest.raises(ValueError):
        tt.insert_tracePos(mock_trace_without_position(), ordering=tt.ORDERING_EXACT)


def test_unknown_ordering_mode_raises():
    with pytest.raises(ValueError):
        tt.insert_tracePos(mock_trace_with_position(), ordering="bogus")


# ----------------------------------- flatten & utils -----------------------------------

def test_flatten_emits_all_operations_with_logIndex():
    trace = mock_trace_with_position()
    tt.insert_tracePos(trace, ordering=tt.ORDERING_EXACT)
    tt.insert_tracePosDepth(trace, ordering=tt.ORDERING_EXACT)
    records = tt.flatten(trace)
    assert len(records) == 5
    assert [r["tracePos"] for r in records] == [1, 2, 3, 4, 5]
    logs = [r for r in records if "logIndex" in r]
    assert [r["logIndex"] for r in sorted(logs, key=lambda r: r["tracePos"])] == [0, 1, 2]
    by_depth = sorted(records, key=lambda r: tt.depth_sort_key(r["tracePosDepth"]))
    assert [r["tracePos"] for r in by_depth] == [1, 2, 3, 4, 5]


def test_flatten_requires_annotation():
    with pytest.raises(ValueError):
        tt.flatten({"type": "CALL", "from": "0xa", "to": "0xb"})


def test_depth_sort_key_avoids_string_trap():
    assert tt.depth_sort_key("1.10") > tt.depth_sort_key("1.2")
    assert sorted(["1.10", "1.2"], key=tt.depth_sort_key) == ["1.2", "1.10"]


# ----------------------------------- live-node tests -----------------------------------

@pytest.mark.skipif(not node_available(), reason="archive node at 127.0.0.1:8081 not reachable")
def test_live_exact_mode_matches_receipt_log_index():
    """Exact mode must order log entries exactly as the receipt's logIndex does,
    on a transaction checked against its receipt."""
    tx = "0xb97455427991b511ffd7074184fd839630752eda7725b607076aa72bb37595b0"
    trace, ok = tt.json_retriever(tx, NODE_URL)
    assert ok
    tt.insert_tracePos(trace, ordering=tt.ORDERING_EXACT)
    tt.insert_tracePosDepth(trace, ordering=tt.ORDERING_EXACT)
    records = tt.flatten(trace)
    logs = [r for r in records if "logIndex" in r]
    assert len(logs) == 7
    assert [r["logIndex"] for r in sorted(logs, key=lambda r: r["tracePos"])] == list(range(7))
    assert all(r["orderExact"] for r in records)


@pytest.mark.skipif(not node_available(), reason="archive node at 127.0.0.1:8081 not reachable")
def test_tx_to_trace_default_produces_paper_order_end_to_end():
    """On a real transaction, the default pipeline output must carry the paper's
    enumeration — for this transaction the known Listing-2 log order
    [1, 0, 3, 2, 5, 4, 6] — while exact mode yields [0..6]."""
    import pandas as pd
    df_txs = pd.DataFrame([
        {"hash": "0xb97455427991b511ffd7074184fd839630752eda7725b607076aa72bb37595b0",
         "timeStamp": 1541416525, "blockNumber": 6668538},
    ])
    df = tt.tx_to_trace(df_txs, NODE_URL)
    logs = df[df["logIndex"].notna()].sort_values("tracePos")
    assert [int(i) for i in logs["logIndex"]] == [1, 0, 3, 2, 5, 4, 6]
    assert (~df["orderExact"]).any()      # approximation is marked in the data

    df_exact = tt.tx_to_trace(df_txs, NODE_URL, ordering=tt.ORDERING_EXACT)
    logs_exact = df_exact[df_exact["logIndex"].notna()].sort_values("tracePos")
    assert [int(i) for i in logs_exact["logIndex"]] == [0, 1, 2, 3, 4, 5, 6]
    # both modes enumerate gaplessly
    for frame in (df, df_exact):
        positions = sorted(frame["tracePos"])
        assert positions == list(range(1, len(frame) + 1))


def _fake_trace(tx_hash):
    """A tiny callTracer-shaped trace, distinct per hash."""
    return {"type": "CALL", "from": "0x01", "to": "0x" + tx_hash[-2:], "gas": "0x1",
            "gasUsed": "0x1", "value": "0x0", "input": "0x",
            "calls": [{"type": "CALL", "from": "0x" + tx_hash[-2:], "to": "0x02",
                       "gas": "0x1", "gasUsed": "0x1", "value": "0x0", "input": "0x"}]}


def test_tx_to_trace_parallel_keeps_input_order(monkeypatch):
    """Responses arriving out of order (random delays) must not change the output:
    parallel == sequential, row for row."""
    import random
    import time as _time
    import pandas as pd

    def slow_retriever(tx_hash, node_url, max_attempts=15):
        _time.sleep(random.uniform(0, 0.02))
        return {"result": _fake_trace(tx_hash)}["result"], True

    monkeypatch.setattr(tt, "json_retriever", slow_retriever)
    df_txs = pd.DataFrame([{"hash": f"0x{i:064x}", "timeStamp": 1000 + i, "blockNumber": i}
                           for i in range(60)])
    sequential = tt.tx_to_trace(df_txs, "x", workers=1)
    parallel = tt.tx_to_trace(df_txs, "x", workers=8)
    assert len(sequential) == 120
    assert sequential.equals(parallel)


def test_tx_to_trace_parallel_fails_fast(monkeypatch):
    import pandas as pd

    def retriever(tx_hash, node_url, max_attempts=15):
        return (_fake_trace(tx_hash), True) if not tx_hash.endswith("05") else ({}, False)

    monkeypatch.setattr(tt, "json_retriever", retriever)
    df_txs = pd.DataFrame([{"hash": f"0x{i:064x}", "timeStamp": 0, "blockNumber": 0}
                           for i in range(20)])
    with pytest.raises(RuntimeError, match="transaction 6 of 20"):
        tt.tx_to_trace(df_txs, "x", workers=8)


@pytest.mark.skipif(not node_available(), reason="archive node at 127.0.0.1:8081 not reachable")
def test_tx_to_trace_fails_fast_on_unretrievable_trace():
    import pandas as pd
    df_txs = pd.DataFrame([{"hash": "0x" + "00" * 32, "timeStamp": 0, "blockNumber": 0}])
    with pytest.raises(RuntimeError):
        tt.tx_to_trace(df_txs, NODE_URL, max_attempts=2)
