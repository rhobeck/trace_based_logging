import os
import socket
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.trace_based_logging.raw_trace_retriever import get_transactions as gt

NODE_URL = "http://127.0.0.1:8081"


def node_available():
    try:
        with socket.create_connection(("127.0.0.1", 8081), timeout=2):
            return True
    except OSError:
        return False


# ----------------------------- request_mediator -----------------------------

def test_request_mediator_distinguishes_empty_from_error():
    ok = {"status": "1", "message": "OK", "result": [{"hash": "0x1", "blockNumber": "10"}]}
    empty = {"status": "0", "message": "No transactions found", "result": []}
    rate_limited = {"status": "0", "message": "NOTOK", "result": "Max rate limit reached"}
    invalid_key = {"status": "0", "message": "NOTOK", "result": "Missing/Invalid API Key"}

    df, s = gt.request_mediator(ok, "0xc")
    assert s == "ok" and len(df) == 1
    assert gt.request_mediator(empty, "0xc")[1] == "empty"
    # the crucial distinction the old code got wrong: these are errors, not "0 transactions"
    assert gt.request_mediator(rate_limited, "0xc")[1] == "error"
    assert gt.request_mediator(invalid_key, "0xc")[1] == "error"


# ----------------------------- get_transactions (mocked V2) -----------------------------

class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload
    def json(self):
        return self._payload


def _install_fake_etherscan(monkeypatch, pages):
    """pages: list of JSON payloads returned in sequence per HTTP call."""
    calls = {"params": []}
    seq = iter(pages)
    def fake_get(url, params=None, timeout=None):
        calls["params"].append(params)
        return _FakeResponse(next(seq))
    monkeypatch.setattr(gt.requests, "get", fake_get)
    return calls


def test_get_transactions_uses_v2_endpoint_and_chainid(monkeypatch):
    calls = _install_fake_etherscan(monkeypatch, [
        {"status": "1", "message": "OK", "result": [{"hash": "0x1", "blockNumber": "10"}]},
    ])
    df = gt.get_transactions(["0xabc"], 0, 100, "normal", "KEY", chainid=1)
    assert len(df) == 1
    p = calls["params"][0]
    assert p["chainid"] == 1 and p["action"] == "txlist" and p["apikey"] == "KEY"


def test_get_transactions_paginates_on_full_page(monkeypatch):
    full = [{"hash": f"0x{i}", "blockNumber": str(i)} for i in range(gt.ETHERSCAN_PAGE_SIZE)]
    tail = [{"hash": "0xtail", "blockNumber": str(gt.ETHERSCAN_PAGE_SIZE)}]
    _install_fake_etherscan(monkeypatch, [
        {"status": "1", "message": "OK", "result": full},
        {"status": "1", "message": "OK", "result": tail},
        {"status": "0", "message": "No transactions found", "result": []},
    ])
    df = gt.get_transactions(["0xabc"], 0, 999999, "normal", "KEY")
    # dedup keeps the boundary block's rows once; tail hash present
    assert "0xtail" in set(df["hash"])
    assert df["hash"].is_unique


def _install_capped_etherscan(monkeypatch, n_txs, cap=1000, per_block=3):
    """Etherscan stand-in with a silent row cap: serves the contract's n_txs
    transactions (per_block per block, ascending) from the requested startblock,
    at most `cap` rows per response, status OK — as observed in Oct 2026."""
    txs = [{"hash": f"0x{i:064x}", "blockNumber": str(i // per_block)} for i in range(n_txs)]
    calls = []

    def fake_get(url, params=None, timeout=None):
        calls.append(params["startblock"])
        rows = [t for t in txs if int(t["blockNumber"]) >= params["startblock"]]
        rows = rows[:min(cap, params["offset"])]
        if not rows:
            return _FakeResponse({"status": "0", "message": "No transactions found",
                                  "result": []})
        return _FakeResponse({"status": "1", "message": "OK", "result": rows})
    monkeypatch.setattr(gt.requests, "get", fake_get)
    return calls


@pytest.mark.parametrize("n_txs,expected_calls", [(2500, 3), (1000, 2), (999, 1)])
def test_get_transactions_pages_past_silent_row_cap(monkeypatch, n_txs, expected_calls):
    """A 1,000-row response to a 10,000-row request is not the last page: all
    transactions are collected; contracts below the cap cost a single request."""
    calls = _install_capped_etherscan(monkeypatch, n_txs)
    df = gt.get_transactions(["0xabc"], 0, 999999, "normal", "KEY")
    assert len(df) == n_txs and df["hash"].is_unique
    assert len(calls) == expected_calls


def test_get_transactions_warns_when_page_is_capped(monkeypatch, caplog):
    import logging
    _install_capped_etherscan(monkeypatch, 2500)
    with caplog.at_level(logging.WARNING):
        gt.get_transactions(["0xabc"], 0, 999999, "internal", "KEY")
    assert any("returned 1000 of 10000 requested rows" in r.getMessage()
               for r in caplog.records)


def test_get_transactions_spaces_requests(monkeypatch):
    """Paging requests go out no faster than the key's tier (min interval)."""
    import time as _time
    starts = []
    calls = _install_capped_etherscan(monkeypatch, 2500)
    real_get = gt.requests.get
    monkeypatch.setattr(gt.requests, "get",
                        lambda *a, **k: (starts.append(_time.monotonic()), real_get(*a, **k))[1])
    gt.get_transactions(["0xabc"], 0, 999999, "normal", "KEY")
    assert len(calls) == 3
    gaps = [b - a for a, b in zip(starts, starts[1:])]
    assert min(gaps) >= gt.ETHERSCAN_MIN_INTERVAL * 0.95


PER_SEC = {"status": "0", "message": "NOTOK",
           "result": "Max calls per sec rate limit reached (3/sec)"}
OK_ONE = {"status": "1", "message": "OK", "result": [{"hash": "0x1", "blockNumber": "10"}]}


def test_per_second_rate_limit_is_retried(monkeypatch, caplog):
    import logging
    monkeypatch.setattr(gt.time, "sleep", lambda s: None)
    calls = _install_fake_etherscan(monkeypatch, [PER_SEC, PER_SEC, OK_ONE])
    with caplog.at_level(logging.WARNING):
        df = gt.get_transactions(["0xabc"], 0, 100, "normal", "KEY")
    assert len(df) == 1 and len(calls["params"]) == 3
    assert sum("transient reply" in r.getMessage() for r in caplog.records) == 2


TIMEOUT = {"status": "0", "message": "Query Timeout occured. Please select a smaller result "
                                     "dataset", "result": None}


def test_query_timeout_is_retried(monkeypatch):
    """Seen on the server (Oct 18): a transient backend timeout; the same query
    succeeded seconds later."""
    monkeypatch.setattr(gt.time, "sleep", lambda s: None)
    calls = _install_fake_etherscan(monkeypatch, [TIMEOUT, OK_ONE])
    df = gt.get_transactions(["0xabc"], 0, 100, "internal", "KEY")
    assert len(df) == 1 and len(calls["params"]) == 2


def test_persistent_query_timeout_fails(monkeypatch):
    monkeypatch.setattr(gt.time, "sleep", lambda s: None)
    calls = _install_fake_etherscan(monkeypatch, [TIMEOUT] * (gt.RATE_LIMIT_RETRIES + 1))
    with pytest.raises(RuntimeError):
        gt.get_transactions(["0xabc"], 0, 100, "internal", "KEY")
    assert len(calls["params"]) == gt.RATE_LIMIT_RETRIES + 1


def test_persistent_per_second_rate_limit_fails(monkeypatch):
    monkeypatch.setattr(gt.time, "sleep", lambda s: None)
    calls = _install_fake_etherscan(monkeypatch, [PER_SEC] * (gt.RATE_LIMIT_RETRIES + 1))
    with pytest.raises(RuntimeError, match="per sec"):
        gt.get_transactions(["0xabc"], 0, 100, "normal", "KEY")
    assert len(calls["params"]) == gt.RATE_LIMIT_RETRIES + 1


def test_daily_rate_limit_is_not_retried(monkeypatch):
    calls = _install_fake_etherscan(monkeypatch, [
        {"status": "0", "message": "NOTOK",
         "result": "Max daily rate limit reached. 100000 (100%) of 100000 calls/day"}])
    with pytest.raises(RuntimeError, match="daily"):
        gt.get_transactions(["0xabc"], 0, 100, "normal", "KEY")
    assert len(calls["params"]) == 1


def test_get_transactions_raises_on_api_error(monkeypatch):
    _install_fake_etherscan(monkeypatch, [
        {"status": "0", "message": "NOTOK", "result": "Max rate limit reached"},
    ])
    with pytest.raises(RuntimeError):
        gt.get_transactions(["0xabc"], 0, 100, "normal", "KEY")


def test_get_transactions_stops_on_stalled_pagination(monkeypatch):
    """A full page confined to a single block must terminate (warn+break), not loop."""
    full_same_block = [{"hash": f"0x{i}", "blockNumber": "5"} for i in range(gt.ETHERSCAN_PAGE_SIZE)]
    _install_fake_etherscan(monkeypatch, [
        {"status": "1", "message": "OK", "result": full_same_block},
    ])
    df = gt.get_transactions(["0xabc"], 5, 100, "normal", "KEY")
    assert len(df) == gt.ETHERSCAN_PAGE_SIZE   # returned once, not re-fetched forever


def test_get_transactions_empty_is_not_error(monkeypatch):
    _install_fake_etherscan(monkeypatch, [
        {"status": "0", "message": "No transactions found", "result": []},
    ])
    df = gt.get_transactions(["0xabc"], 0, 100, "internal", "KEY")
    assert df.empty


# ----------------------------- event discovery (fake node) -----------------------------

def _fake_w3(logs):
    """Minimal web3 stand-in: get_logs filters `logs` by address list and range,
    and records every filter it receives."""
    calls = []

    class Eth:
        def get_logs(self, f):
            addrs = f["address"] if isinstance(f["address"], list) else [f["address"]]
            calls.append((f["fromBlock"], f["toBlock"], len(addrs)))
            wanted = {a.lower() for a in addrs}
            return [l for l in logs if l["address"].lower() in wanted
                    and f["fromBlock"] <= l["blockNumber"] <= f["toBlock"]]

        def get_block(self, bn):
            return {"timestamp": 1_000_000 + bn}

    class W3:
        eth = Eth()
    return W3(), calls


A1 = "0x1985365e9f78359a9b6ad760e32412f4a445e862"
A2 = "0x75228dce4d82566d93068a8d5d49435216551599"
A3 = "0x57f1c2953630056aaadb9bfbda05369e6af7872b"
FAKE_LOGS = [{"address": a, "blockNumber": bn, "transactionHash": bytes([i]) * 32}
             for i, (a, bn) in enumerate([(A1, 105), (A2, 2_340), (A2, 4_999), (A3, 3_001),
                                          (A1, 4_000)])]


def test_events_batched_equal_one_by_one(monkeypatch):
    w3, calls = _fake_w3(FAKE_LOGS)
    monkeypatch.setattr(gt, "make_web3", lambda url: w3)
    single = gt.get_transactions_by_events("x", [A1, A2, A3], 0, 5_000, chunk_size=1_000,
                                           address_batch=1)
    n_single = len(calls)
    batched = gt.get_transactions_by_events("x", [A1, A2, A3], 0, 5_000, chunk_size=1_000,
                                            address_batch=100)
    key = ["hash", "blockNumber", "contractAddress"]
    assert len(batched) == len(FAKE_LOGS)
    assert (single.sort_values(key).reset_index(drop=True)
            .equals(batched.sort_values(key).reset_index(drop=True)))
    assert n_single == 18 and len(calls) - n_single == 6     # 3x6 chunks vs 1x6


def test_events_start_at_creation_block(monkeypatch):
    """A contract created at block 3,000 is searched from there; the result is the
    same, because a contract emits nothing before it exists."""
    w3, calls = _fake_w3(FAKE_LOGS)
    monkeypatch.setattr(gt, "make_web3", lambda url: w3)
    df = gt.get_transactions_by_events("x", [A3], 0, 5_000, chunk_size=1_000,
                                       start_blocks={A3: 3_000})
    assert list(df["blockNumber"]) == [3_001]
    assert [c[0] for c in calls] == [3_000, 4_000, 5_000]       # not from block 0
    # a batch starts at its earliest member; creation before min_block is clamped
    calls.clear()
    df = gt.get_transactions_by_events("x", [A1, A2, A3], 2_000, 5_000, chunk_size=1_000,
                                       start_blocks={A2: 100, A3: 4_000})
    assert min(c[0] for c in calls) == 2_000
    assert set(df["blockNumber"]) == {2_340, 4_999, 3_001, 4_000}


def test_events_report_progress_within_a_batch(monkeypatch, caplog):
    """A long batch reports its position before it ends; the result is unchanged."""
    import logging
    w3, _ = _fake_w3(FAKE_LOGS)
    monkeypatch.setattr(gt, "make_web3", lambda url: w3)
    monkeypatch.setattr(gt, "GETLOGS_PROGRESS_INTERVAL", 0)
    with caplog.at_level(logging.INFO):
        df = gt.get_transactions_by_events("x", [A1, A2, A3], 0, 3_999, chunk_size=1_000)
    progress = [r.getMessage() for r in caplog.records if " at block " in r.getMessage()]
    assert progress[0] == ("EVENT-BASED transactions: contracts 1-3/3 at block 999 "
                           "(25% of 0-3999), 1 rows so far")
    assert len(progress) == 4
    assert len(df) == 3                      # logs at 105, 2,340, 3,001


# ----------------------------- live node: event discovery -----------------------------

@pytest.mark.skipif(not node_available(), reason="archive node at 127.0.0.1:8081 not reachable")
def test_get_transactions_by_events_matches_known_window():
    """REP token 9,000,000-9,004,999 has 618 logs (verified externally and on-chain)."""
    df = gt.get_transactions_by_events(
        NODE_URL, ["0x1985365e9f78359a9b6ad760e32412f4a445e862"], 9000000, 9004999)
    assert len(df) == 618
    assert {"blockNumber", "timeStamp", "hash", "contractAddress"}.issubset(df.columns)
    # hexbytes >= 1.0 dropped the 0x prefix from .hex(); the pipeline contract is
    # 0x-prefixed hashes everywhere
    assert df["hash"].str.startswith("0x").all()


@pytest.mark.skipif(not node_available(), reason="archive node at 127.0.0.1:8081 not reachable")
def test_event_fetcher_splits_wide_range():
    """A 5000-block range exceeds the node's getLogs cap; adaptive splitting must still
    return the complete set (same 618 logs as the chunked discovery above)."""
    from web3 import Web3
    w3 = Web3(Web3.HTTPProvider(NODE_URL))
    logs = gt.event_fetcher(w3, "0x1985365e9f78359a9b6ad760e32412f4a445e862", 9000000, 9004999)
    assert len(logs) == 618


@pytest.mark.skipif(not node_available(), reason="archive node at 127.0.0.1:8081 not reachable")
def test_events_batched_equal_one_by_one_on_chain():
    """On the node: one filter over several addresses returns exactly the union of the
    per-address results (REP + the Augur main contract, a busy window)."""
    contracts = ["0x1985365e9f78359a9b6ad760e32412f4a445e862",
                 "0x75228dce4d82566d93068a8d5d49435216551599",
                 "0x57f1c2953630056aaadb9bfbda05369e6af7872b"]
    single = gt.get_transactions_by_events(NODE_URL, contracts, 9000000, 9004999,
                                           address_batch=1)
    batched = gt.get_transactions_by_events(NODE_URL, contracts, 9000000, 9004999)
    key = ["hash", "blockNumber", "contractAddress"]
    assert len(single) >= 618
    assert (single.sort_values(key).reset_index(drop=True)
            .equals(batched.sort_values(key).reset_index(drop=True)))
