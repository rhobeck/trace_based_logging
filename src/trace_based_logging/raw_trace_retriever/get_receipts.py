"""Fetch one transaction receipt per discovered transaction and annotate the trace
operations with what the receipt says about the transaction.

Runs after discovery, over the set of discovered transaction hashes, and supplies:

  * transactionIndex – the position of the transaction in its block; with the block
    number and the position of an operation in the trace it gives the order of
    operations (Section 7 of the paper).
  * status           – whether the transaction succeeded (1) or reverted (0). The
    receipt is authoritative: a sub-call can revert while the transaction succeeds,
    so the trace frames alone do not tell.
  * logCount         – the number of log entries in the receipt; the trace must
    contain exactly as many, which the pipeline checks.

Receipts are cached in resources/cache/receipts.parquet: an existing cache is loaded
and only the missing hashes are fetched, so reruns do not query the node again.
"""

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests

from src.trace_based_logging.logging_config import setup_logging

logger = setup_logging()

MAX_RETRIES = 8
RETRY_DELAY = 0.5
COLUMNS = ["txHash", "transactionIndex", "status", "logCount", "blockNumber"]

_thread_local = threading.local()


def _session():
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        _thread_local.session = s
    return s


def _fetch_one(tx_hash, node_url):
    for _ in range(MAX_RETRIES):
        try:
            r = _session().post(
                node_url,
                json={"jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionReceipt",
                      "params": [tx_hash]},
                timeout=30,
            )
            res = r.json().get("result")
            if res:
                return (tx_hash, int(res["transactionIndex"], 16), int(res["status"], 16),
                        len(res["logs"]), int(res["blockNumber"], 16))
        except Exception:
            time.sleep(RETRY_DELAY)
    return (tx_hash, None, None, None, None)


def get_receipts(hashes, node_url, cache_path=None, max_workers=4):
    """Fetch receipt-derived fields for the given transaction hashes.

    Args:
        hashes: iterable of transaction hash strings (the discovered transaction set).
        node_url: archive node URL.
        cache_path: optional parquet path; existing rows are reused and the (possibly
            extended) table is written back.
        max_workers: concurrent request workers.

    Returns:
        pd.DataFrame with columns COLUMNS, one row per unique hash.

    Raises:
        RuntimeError: if any receipt could not be retrieved (fail-fast: no silent gaps).
    """
    wanted = sorted(set(hashes))

    cached = pd.DataFrame(columns=COLUMNS)
    if cache_path and os.path.exists(cache_path):
        cached = pd.read_parquet(cache_path)
        logger.info(f"Loaded {len(cached)} cached receipts from {cache_path}")

    have = set(cached.loc[cached["transactionIndex"].notna(), "txHash"])
    missing = [h for h in wanted if h not in have]
    logger.info(f"Receipts: {len(wanted)} wanted, {len(have & set(wanted))} cached, "
                f"{len(missing)} to fetch")

    fetched_rows = []
    if missing:
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for i, row in enumerate(
                    ex.map(lambda h: _fetch_one(h, node_url), missing, chunksize=200), 1):
                fetched_rows.append(row)
                if i % 50000 == 0:
                    logger.info(f"Receipts: {i}/{len(missing)} ({i/(time.time()-t0):.0f}/s)")

    fetched = pd.DataFrame(fetched_rows, columns=COLUMNS)
    combined = pd.concat([cached, fetched], ignore_index=True)
    combined = combined.drop_duplicates(subset="txHash", keep="last").reset_index(drop=True)

    result = combined[combined["txHash"].isin(wanted)].reset_index(drop=True)
    unresolved = result[result["transactionIndex"].isna()]["txHash"].tolist()
    if unresolved:
        raise RuntimeError(
            f"Receipt retrieval failed for {len(unresolved)} transaction(s); "
            f"first: {unresolved[:5]}")

    if cache_path:
        combined.to_parquet(cache_path, index=False)
        logger.info(f"Wrote {len(combined)} receipts to {cache_path}")

    # integer dtypes (no NaN remains after the fail-fast check)
    for col in ["transactionIndex", "status", "logCount", "blockNumber"]:
        result[col] = result[col].astype("int64")
    return result
