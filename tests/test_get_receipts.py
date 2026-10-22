import os
import socket
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.trace_based_logging.raw_trace_retriever import get_receipts as gr

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
NODE_URL = "http://127.0.0.1:8081"
CACHE = os.path.join(REPO, "resources", "cache", "receipts.parquet")


def node_available():
    try:
        with socket.create_connection(("127.0.0.1", 8081), timeout=2):
            return True
    except OSError:
        return False


def cache_available():
    return os.path.exists(CACHE)


@pytest.mark.skipif(not cache_available(), reason="cached receipts parquet not present")
def test_cache_served_without_node():
    """With every wanted hash already cached, no node access is needed. Point node_url
    at an unusable address to prove nothing is fetched."""
    cached = pd.read_parquet(CACHE)
    sample = cached["txHash"].head(1000).tolist()
    out = gr.get_receipts(sample, node_url="http://127.0.0.1:1", cache_path=CACHE)
    assert len(out) == 1000
    assert list(out.columns) == gr.COLUMNS
    assert out["transactionIndex"].notna().all()
    assert set(out["status"].unique()).issubset({0, 1})


@pytest.mark.skipif(not cache_available(), reason="cached receipts parquet not present")
def test_fail_fast_on_unresolvable_without_node():
    """A hash absent from cache, with no reachable node, must raise rather than return
    a row with a null transactionIndex."""
    with pytest.raises(RuntimeError):
        gr.get_receipts(["0x" + "ab" * 32], node_url="http://127.0.0.1:1",
                        cache_path=CACHE)


LOG_EVENTS = os.path.join(REPO, "resources", "output_augur_11229577", "transformation",
                          "log_paper", "events.parquet")


@pytest.mark.skipif(not (cache_available() and os.path.exists(LOG_EVENTS)),
                    reason="cached receipts or full-scale log not present")
def test_cache_matches_published_revert_counts():
    """Sanity anchor: the chain-truth revert count of the corrected case study
    (9,372 reverted of 472,377 transactions: the paper's 9,370 of 472,375 plus
    the two additional reverted transactions), distinct from the frame-flagged
    count. The cache is shared and additive across runs, so the anchor is
    evaluated on the case-study population — the full-scale log's transactions
    — not on the cache size."""
    cached = pd.read_parquet(CACHE)
    assert cached["transactionIndex"].notna().all()
    assert len(cached) >= 472377
    pop = pd.read_parquet(LOG_EVENTS, columns=["txHash"])["txHash"].unique()
    assert len(pop) == 472377
    sub = cached[cached["txHash"].isin(set(pop))]
    assert len(sub) == 472377
    assert int((sub["status"] == 0).sum()) == 9372


@pytest.mark.skipif(not (node_available() and cache_available()),
                    reason="needs node and cache")
def test_live_fetch_matches_cache_for_known_tx():
    """Fetching a known tx fresh from the node must equal its cached row."""
    tx = "0xb97455427991b511ffd7074184fd839630752eda7725b607076aa72bb37595b0"
    row = gr._fetch_one(tx, NODE_URL)
    cached = pd.read_parquet(CACHE).set_index("txHash").loc[tx]
    assert row[1] == int(cached["transactionIndex"])   # transactionIndex
    assert row[2] == int(cached["status"])             # status
    assert row[3] == int(cached["logCount"])           # logCount
