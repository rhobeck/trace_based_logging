import time
import pandas as pd
import requests
from web3 import Web3
from src.trace_based_logging.logging_config import setup_logging
from src.trace_based_logging.raw_trace_retriever.trace_retriever_utils import make_web3

"""
Fetch transactions relevant to a set of contracts within a block range, from three
sources (as described in Section 6 of the paper):

  * normal transactions   (Etherscan V2, action=txlist)
  * internal transactions (Etherscan V2, action=txlistinternal)
  * transactions by events (node eth_getLogs)

An optional node-only discovery path (get_transactions_by_traces) uses the node's
trace_filter instead of Etherscan; the paper names it as future work, the published
data do not use it.

Etherscan migrated to a single V2 endpoint (api.etherscan.io/v2/api) taking a `chainid`
parameter; the V1 endpoint used previously is retired. Response contract is unchanged:
{"status": "1"|"0", "message": ..., "result": [...]|str}.

Functions:
    send_api_request  – one Etherscan V2 request, with retries.
    request_mediator  – parse an Etherscan JSON payload into a DataFrame.
    get_transactions  – normal or internal transactions for a list of contracts.
    get_transactions_by_events – transactions discovered via emitted events (node).
    event_fetcher     – eth_getLogs for one filter, with retries and range-splitting.
    get_transactions_by_traces – node-only discovery via trace_filter (optional).
"""

logger = setup_logging()

ETHERSCAN_V2_URL = "https://api.etherscan.io/v2/api"
ETHERSCAN_PAGE_SIZE = 10000       # rows requested per page (offset)
# Etherscan may return fewer rows than requested without saying so (observed Oct
# 2026: 1,000 per page, status OK). A page this size or larger may be truncated, so
# paging continues; only a smaller page is certainly the last.
ETHERSCAN_FULL_PAGE = 1000
# Pacing: minimum spacing of request starts (the key's tier allows 3 calls/s,
# observed Oct 2026); replaces a fixed pause per contract and also covers paging.
ETHERSCAN_MIN_INTERVAL = 0.34
# Transient replies — "Max calls per sec rate limit reached" and "Query Timeout
# occured" (Etherscan backend overload, seen Oct 2026) — are retried this often
# (rising pause, logged) before failing. The daily limit and other errors fail fast.
RATE_LIMIT_RETRIES = 10
_last_request = [0.0]              # monotonic start time of the previous request
DELAY = 5                          # base backoff between retries (seconds)
MAX_RETRIES = 5
# eth_getLogs block-range cap. Public/free archive endpoints and this project's Erigon
# reject overly wide ranges; the fetcher also halves the range adaptively on rejection.
GETLOGS_CHUNK_SIZE = 1000
# addresses per eth_getLogs filter (the filter takes a list; each log names its emitter)
GETLOGS_ADDRESS_BATCH = 100
# seconds between progress lines within one batch (a batch over the full range runs
# for an hour or more)
GETLOGS_PROGRESS_INTERVAL = 300


def send_api_request(contract_address, min_block, max_block, internal_flag,
                     etherscan_api_key, chainid=1, page_size=ETHERSCAN_PAGE_SIZE):
    """One Etherscan V2 request for normal or internal transactions, with retries.

    Request starts are spaced ETHERSCAN_MIN_INTERVAL apart (paging included). The
    transient replies (per-second rate limit, query timeout) are retried up to
    RATE_LIMIT_RETRIES times; every other payload — the daily limit included — is
    returned for the caller to judge.
    Returns the parsed JSON dict, or raises after MAX_RETRIES failed attempts.
    """
    action = {"normal": "txlist", "internal": "txlistinternal"}[internal_flag]
    parameters = {
        "chainid": chainid,
        "module": "account",
        "action": action,
        "address": contract_address,
        "startblock": min_block,
        "endblock": max_block,
        "offset": page_size,
        "sort": "asc",
        "apikey": etherscan_api_key,
    }
    attempt, rate_limit_hits = 0, 0
    while attempt < MAX_RETRIES:
        wait = _last_request[0] + ETHERSCAN_MIN_INTERVAL - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_request[0] = time.monotonic()
        try:
            payload = requests.get(ETHERSCAN_V2_URL, params=parameters, timeout=30).json()
        except (requests.exceptions.RequestException, ValueError) as e:
            attempt += 1
            logger.error(f"Etherscan request error for {contract_address} "
                         f"(attempt {attempt}/{MAX_RETRIES}): {e}")
            time.sleep(DELAY * attempt)
            continue
        result = str(payload.get("result", ""))
        message = str(payload.get("message", ""))
        timeout = "query timeout" in message.lower()      # backend overload (message field)
        if payload.get("status") == "0" and ("per sec" in result.lower() or timeout) \
                and rate_limit_hits < RATE_LIMIT_RETRIES:
            rate_limit_hits += 1
            logger.warning(f"Etherscan transient reply for {contract_address} "
                           f"(retry {rate_limit_hits}/{RATE_LIMIT_RETRIES}): "
                           f"{message if timeout else result}")
            time.sleep((2.0 if timeout else 0.5) * rate_limit_hits)
            continue
        return payload        # anything else (incl. a persistent rate limit): caller decides
    raise RuntimeError(f"Etherscan request failed after {MAX_RETRIES} attempts "
                       f"for {contract_address}")


def request_mediator(txs_json, contract_address):
    """Parse an Etherscan JSON payload into a DataFrame of transactions.

    Returns (df, status) where status is one of:
        "ok"        – result rows returned (df may be empty on the final page)
        "empty"     – status 0 with the "no transactions found" message
        "error"     – any other non-success payload (rate limit, invalid key, ...)
    The caller decides how to react; errors are never silently treated as "no data".
    """
    status = txs_json.get("status")
    result = txs_json.get("result")
    if status == "1" and isinstance(result, list):
        return pd.DataFrame(result), "ok"
    message = str(txs_json.get("message", ""))
    if status == "0" and isinstance(result, list) and not result:
        logger.debug(f"0 transactions for {contract_address}")
        return pd.DataFrame(), "empty"
    if status == "0" and "no transactions found" in message.lower():
        return pd.DataFrame(), "empty"
    logger.error(f"Etherscan non-success payload for {contract_address}: "
                 f"message={message!r} result={str(result)[:120]!r}")
    return pd.DataFrame(), "error"


def get_transactions(list_lx, min_block, max_block, internal_flag, etherscan_api_key,
                     chainid=1):
    """Fetch normal or internal transactions for a list of contract addresses.

    Handles Etherscan's rows-per-response cap by advancing the start block to the
    last block seen and re-querying (never start+1, so no in-block transactions are
    lost). The cap is not taken from the request: any page of ETHERSCAN_FULL_PAGE
    rows or more is treated as possibly truncated. Fails fast on genuine API errors
    rather than proceeding with partial data.
    """
    if internal_flag not in ("normal", "internal"):
        raise ValueError(f"internal_flag must be 'normal' or 'internal', got {internal_flag!r}")

    frames = []
    contracts = list(list_lx)
    for idx, contract_address in enumerate(contracts, start=1):
        window_min = min_block
        while True:
            payload = send_api_request(contract_address, window_min, max_block,
                                       internal_flag, etherscan_api_key, chainid)
            df_page, status = request_mediator(payload, contract_address)
            if status == "error":
                raise RuntimeError(
                    f"Etherscan error for {contract_address} "
                    f"(blocks {window_min}-{max_block}): {payload.get('result')}")
            if status == "empty" or df_page.empty:
                break
            frames.append(df_page)
            logger.info(f"{internal_flag.upper()} transactions: contract {idx}/{len(contracts)} "
                        f"page of {len(df_page)} rows for {contract_address}")
            if len(df_page) < ETHERSCAN_FULL_PAGE:
                break
            if len(df_page) < ETHERSCAN_PAGE_SIZE:
                logger.warning(f"Etherscan returned {len(df_page)} of {ETHERSCAN_PAGE_SIZE} "
                               f"requested rows for {contract_address}; paging on")
            # more rows may exist in the last block of this page; resume from it
            last_block = pd.to_numeric(df_page["blockNumber"]).max()
            if last_block <= window_min:
                # no forward progress possible (full page within one block); stop rather
                # than loop forever
                logger.warning(f"Pagination stalled at block {last_block} for "
                               f"{contract_address}; stopping to avoid a loop.")
                break
            window_min = int(last_block)

    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df.drop_duplicates(subset="hash", keep="last", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def event_fetcher(w3, address, from_block, to_block):
    """eth_getLogs for one address (or a list of addresses) and range, with retries
    and adaptive range-splitting.

    Splits the range and recurses when the node rejects it as too wide, so a single
    GETLOGS_CHUNK_SIZE need not be tuned per endpoint.
    """
    if isinstance(address, (list, tuple)):
        checksummed = [Web3.to_checksum_address(a) for a in address]
        label = f"{len(address)} addresses from {address[0]}"
    else:
        checksummed = Web3.to_checksum_address(address)
        label = address
    for attempt in range(MAX_RETRIES):
        try:
            return w3.eth.get_logs({"address": checksummed,
                                    "fromBlock": from_block, "toBlock": to_block})
        except Exception as e:
            msg = str(e).lower()
            # match only range-width rejections; a bare "limit"/"range" would also match
            # rate-limit messages and trigger useless splitting
            too_wide = ("block range" in msg or "too many" in msg or "exceeds" in msg)
            if too_wide and to_block > from_block:
                mid = (from_block + to_block) // 2
                return (event_fetcher(w3, address, from_block, mid)
                        + event_fetcher(w3, address, mid + 1, to_block))
            logger.error(f"eth_getLogs error {label} {from_block}-{to_block} "
                         f"(attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            time.sleep(DELAY * (attempt + 1))
    raise RuntimeError(f"eth_getLogs failed for {label} {from_block}-{to_block}")


def get_transactions_by_events(node_url, contracts, min_block, max_block,
                               chunk_size=GETLOGS_CHUNK_SIZE, start_blocks=None,
                               address_batch=GETLOGS_ADDRESS_BATCH):
    """Discover transactions by the events the given contracts emitted (node getLogs).

    Contracts are queried in batches of address_batch per filter. start_blocks
    (address -> block of its earliest CREATE, optional) lets a contract's search
    begin at its creation instead of min_block: a contract emits no events before
    it exists, so the result is unchanged. Contracts are sorted by start block, and
    a batch starts at the earliest start among its members.
    """
    w3 = make_web3(node_url)
    rows = []
    start_blocks = start_blocks or {}
    starts = {a: max(min_block, start_blocks.get(a.lower(), min_block))
              for a in contracts}
    contracts = sorted(contracts, key=lambda a: (starts[a], a))
    block_ts = {}
    for first in range(0, len(contracts), address_batch):
        batch = contracts[first:first + address_batch]
        last_progress = time.monotonic()
        for chunk_start in range(starts[batch[0]], max_block + 1, chunk_size):
            chunk_end = min(chunk_start + chunk_size - 1, max_block)
            logs = event_fetcher(w3, batch, chunk_start, chunk_end)
            for log in logs:
                bn = log["blockNumber"]
                if bn not in block_ts:
                    block_ts[bn] = w3.eth.get_block(bn)["timestamp"]
                # hexbytes >= 1.0 dropped the 0x prefix from .hex(); normalize explicitly
                tx_hash = log["transactionHash"].hex()
                if not tx_hash.startswith("0x"):
                    tx_hash = "0x" + tx_hash
                rows.append({"blockNumber": bn, "timeStamp": block_ts[bn],
                             "hash": tx_hash,
                             "contractAddress": log["address"]})
            if time.monotonic() - last_progress >= GETLOGS_PROGRESS_INTERVAL:
                last_progress = time.monotonic()
                share = (chunk_end - starts[batch[0]] + 1) / (max_block - starts[batch[0]] + 1)
                logger.info(f"EVENT-BASED transactions: contracts {first + 1}-{first + len(batch)}"
                            f"/{len(contracts)} at block {chunk_end} ({share:.0%} of "
                            f"{starts[batch[0]]}-{max_block}), {len(rows)} rows so far")
        logger.info(f"EVENT-BASED transactions: contracts {first + 1}-{first + len(batch)}"
                    f"/{len(contracts)} from block {starts[batch[0]]}, "
                    f"{len(rows)} rows so far")
    return pd.DataFrame(rows)


def get_transactions_by_traces(node_url, contracts, min_block, max_block,
                               chunk_size=GETLOGS_CHUNK_SIZE):
    """Node-only discovery via trace_filter: transactions where any given contract was
    sender or receiver of a (sub-)call. Replaces the Etherscan normal+internal pair
    without a third-party dependence. Optional; not used for the published data.
    """
    contracts_cs = [Web3.to_checksum_address(c) for c in contracts]
    hashes = {}
    for chunk_start in range(min_block, max_block + 1, chunk_size):
        chunk_end = min(chunk_start + chunk_size - 1, max_block)
        for role in ("fromAddress", "toAddress"):
            params = {"fromBlock": hex(chunk_start), "toBlock": hex(chunk_end),
                      role: contracts_cs}
            for attempt in range(MAX_RETRIES):
                try:
                    resp = requests.post(node_url, json={
                        "jsonrpc": "2.0", "id": 1, "method": "trace_filter",
                        "params": [params]}, timeout=60).json()
                    if "result" in resp:
                        for tr in resp["result"]:
                            hashes[tr["transactionHash"]] = tr["blockNumber"]
                        break
                    raise RuntimeError(resp.get("error"))
                except Exception as e:
                    logger.error(f"trace_filter {chunk_start}-{chunk_end} {role} "
                                 f"(attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                    time.sleep(DELAY * (attempt + 1))
            else:
                raise RuntimeError(f"trace_filter failed {chunk_start}-{chunk_end} {role}")
    return pd.DataFrame([{"hash": h, "blockNumber": b} for h, b in hashes.items()])
