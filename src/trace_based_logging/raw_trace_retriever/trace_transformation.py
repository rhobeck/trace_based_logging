import pandas as pd
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from src.trace_based_logging.logging_config import setup_logging

"""
This module retrieves transaction traces from a blockchain node and transforms them into
per-operation records with their order of execution.

Functions:
    json_retriever(tx_hash, node_url, max_attempts=15):
        Retrieves the callTracer JSON for a transaction hash, with retries.

    tx_to_trace(df_txs_lx, node_url, ordering=ORDERING_PAPER, max_attempts=15,
                workers=TRACE_WORKERS):
        Converts a DataFrame of transactions into a DataFrame of transaction trace
        operations (call frames and log entries) annotated with tracePos/tracePosDepth.
        Traces are fetched concurrently; the output is independent of `workers`.

    insert_tracePos(data, trace_pos_counter=None, ordering=ORDERING_PAPER):
        Annotates every operation in a trace with 'tracePos', its 1-based position in
        the enumeration of the transaction (see Ordering modes below).

    insert_tracePosDepth(item, parent_index='', ordering=ORDERING_PAPER):
        Annotates every operation with 'tracePosDepth', a hierarchical identifier
        ("1.3.2") reflecting the tree structure of the trace.

    flatten(trace_json_lx):
        Flattens an annotated trace into a list of per-operation records, one per
        call frame and per log entry.

    depth_sort_key(trace_pos_depth):
        Sortable tuple form of a tracePosDepth string ('1.10.2' -> (1, 10, 2)).

Ordering modes:

    ORDERING_PAPER ("paper", the default, used for the published logs): the
        enumeration of Listing 2 of the paper — a frame's sub-calls are numbered
        before the frame's log entries. Within a frame that both emits log entries
        and makes sub-calls, the true interleaving of the two is not represented;
        such operations carry orderExact = False. The per-log 'position' field of
        newer tracers, which resolves it, did not exist when the data of the paper
        were collected.

    ORDERING_EXACT ("exact", for validation, not used for the published logs): the
        true execution order, reconstructed by interleaving a frame's log entries
        among its sub-calls via the tracer's per-log 'position' field. Requires a
        client that emits 'position' (Geth 1.13.5 or later, Erigon 2025 or later);
        raises otherwise. The tests use it with the receipt's logIndex as ground
        truth.
"""

logger = setup_logging()

# keys of a call frame that hold nested operations rather than frame attributes
NESTED_KEYS = ("calls", "logs")

ORDERING_PAPER = "paper"
ORDERING_EXACT = "exact"

# concurrent debug_traceTransaction requests in tx_to_trace (16-core node host:
# leaves headroom for the node itself and the pipeline's own processing)
TRACE_WORKERS = 8


def json_retriever(tx_hash, node_url, max_attempts=15):
    """
    Retrieves the callTracer JSON (withLog enabled) for a transaction hash.

    Args:
        tx_hash (str): The transaction hash to retrieve the trace for.
        node_url (str): The URL of the blockchain node to query.
        max_attempts (int): Maximum number of attempts for the request.

    Returns:
        tuple: (response_json, json_flag) where json_flag is False if no valid trace
        could be retrieved after max_attempts.
    """
    headers = {'Content-type': 'application/json'}
    parameters = {
        "jsonrpc": "2.0",
        "method": "debug_traceTransaction",
        "params": [tx_hash, {"tracer": 'callTracer', "tracerConfig": {"withLog": True}}],
        "id": 1
    }
    attempts = 0
    while attempts < max_attempts:
        try:
            response = requests.post(node_url, json=parameters, headers=headers,
                                     timeout=300)
            response_json = response.json()
            if response_json and isinstance(response_json.get("result"), dict) and "type" in response_json["result"]:
                return response_json, True
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception for tx_hash {tx_hash}: {e}")
        except ValueError as e:
            logger.error(f"JSON decoding error for tx_hash {tx_hash}: {e}")
        attempts += 1
        time.sleep(min(2 ** attempts * 0.1, 5))

    logger.error(f"Max attempts reached. Invalid tx hash: {tx_hash}.")
    return {}, False


def _hex_to_int(value):
    if isinstance(value, str) and value.startswith("0x"):
        return int(value, 16)
    return value


def _children_in_order(frame, ordering):
    """Return one frame's sub-calls and logs as ("call"|"log", node) pairs in the
    order prescribed by the ordering mode, plus an exactness flag for the children.

    ORDERING_PAPER: sub-calls first, then logs (Listing 2); exact only when the frame
    emits no logs (a calls-only frame has no interleaving ambiguity).
    ORDERING_EXACT: logs interleaved among sub-calls via the per-log 'position' field;
    raises ValueError when the trace does not carry it.
    """
    if ordering not in (ORDERING_PAPER, ORDERING_EXACT):
        raise ValueError(f"Unknown ordering mode: {ordering!r} "
                         f"(expected {ORDERING_PAPER!r} or {ORDERING_EXACT!r})")
    calls = frame.get("calls", [])
    logs = frame.get("logs", [])
    if not logs:
        return [("call", c) for c in calls], True
    if ordering == ORDERING_PAPER:
        return [("call", c) for c in calls] + [("log", l) for l in logs], False
    if not all("position" in l for l in logs):
        raise ValueError(
            "Exact ordering requested but the trace logs lack the 'position' field. "
            "Re-trace with a client that provides it (Geth >= 1.13.5 / Erigon 2025+), "
            "or use the default paper ordering."
        )
    by_position = {}
    for l in logs:
        by_position.setdefault(_hex_to_int(l["position"]), []).append(l)
    ordered = []
    for i, call in enumerate(calls):
        for l in by_position.pop(i, []):
            ordered.append(("log", l))
        ordered.append(("call", call))
    for pos in sorted(by_position):
        for l in by_position[pos]:
            ordered.append(("log", l))
    return ordered, True


def insert_tracePos(data, trace_pos_counter=None, ordering=ORDERING_PAPER):
    """
    Annotates every operation (call frame and log entry) with 'tracePos': its 1-based
    position in the transaction's enumeration under the given ordering mode. Also sets
    'orderExact' (False on operations whose relative order is approximated by the
    paper enumeration).

    Args:
        data (dict): A trace result, or the full JSON-RPC response containing 'result'.
        trace_pos_counter (list of int, optional): single-element counter carried across
            recursive calls; a fresh counter is created when omitted.
        ordering (str): ORDERING_PAPER (default) or ORDERING_EXACT (validation only).
    """
    if trace_pos_counter is None:
        trace_pos_counter = [0]

    if 'result' in data:
        insert_tracePos(data['result'], [0], ordering)
        return data

    trace_pos_counter[0] += 1
    data['tracePos'] = trace_pos_counter[0]
    data.setdefault('orderExact', True)

    ordered, exact = _children_in_order(data, ordering)
    for kind, node in ordered:
        node['orderExact'] = exact
        if kind == "call":
            insert_tracePos(node, trace_pos_counter, ordering)
        else:
            trace_pos_counter[0] += 1
            node['tracePos'] = trace_pos_counter[0]
    return data


def insert_tracePosDepth(item, parent_index='', ordering=ORDERING_PAPER):
    """
    Annotates every operation with 'tracePosDepth': a hierarchical identifier ("1.3.2")
    carrying the parent's identifier plus the operation's 1-based rank among the
    parent's children under the given ordering mode.

    Args:
        item (dict): A trace result, or the full JSON-RPC response containing 'result'.
        parent_index (str): identifier of the parent operation ('' at the root).
        ordering (str): ORDERING_PAPER (default) or ORDERING_EXACT (validation only).
    """
    if item is None:
        return item
    if 'result' in item:
        insert_tracePosDepth(item['result'], parent_index, ordering)
        return item

    item['tracePosDepth'] = parent_index if parent_index else '1'

    ordered, _exact = _children_in_order(item, ordering)
    for rank, (kind, node) in enumerate(ordered, start=1):
        child_id = f"{item['tracePosDepth']}.{rank}"
        if kind == "call":
            insert_tracePosDepth(node, child_id, ordering)
        else:
            node['tracePosDepth'] = child_id
    return item


def depth_sort_key(trace_pos_depth):
    """Sortable form of a tracePosDepth string: '1.10.2' -> (1, 10, 2).

    Guards against the lexicographic trap ('1.10' < '1.2' as strings).
    """
    return tuple(int(part) for part in str(trace_pos_depth).split("."))


def flatten(trace_json_lx):
    """
    Flattens an annotated trace (see insert_tracePos / insert_tracePosDepth) into a
    list of per-operation records, one per call frame or log entry, ordered by their
    assigned tracePos.

    Replaces the earlier flatten/flatten_nested/explode_df combination: records are
    emitted directly from the JSON tree instead of via repeated DataFrame explosion,
    which also removes the row-identity hazards of index-based processing.

    Args:
        trace_json_lx (dict): A trace result, or the full JSON-RPC response.

    Returns:
        list[dict]: per-operation records carrying the frame/log attributes plus
        tracePos, tracePosDepth and orderExact. Log records expose the tracer's
        receipt-level log index as 'logIndex' where available (validation data; not a
        published-log attribute).
    """
    root = trace_json_lx['result'] if 'result' in trace_json_lx else trace_json_lx
    if "tracePos" not in root:
        raise ValueError("flatten() requires an annotated trace; call insert_tracePos() "
                         "and insert_tracePosDepth() first")
    records = []

    def emit(node, is_log):
        rec = {k: v for k, v in node.items() if k not in NESTED_KEYS}
        if is_log and "index" in rec:
            rec["logIndex"] = _hex_to_int(rec.pop("index"))
        rec.pop("position", None)
        records.append(rec)

    def visit(frame):
        emit(frame, is_log=False)
        for c in frame.get("calls", []):
            visit(c)
        for l in frame.get("logs", []):
            emit(l, is_log=True)

    visit(root)
    records.sort(key=lambda r: r["tracePos"])
    return records


def tx_to_trace(df_txs_lx, node_url, ordering=ORDERING_PAPER, max_attempts=15,
                workers=TRACE_WORKERS):
    """
    Converts a DataFrame of transactions into a DataFrame of transaction trace
    operations by retrieving and flattening the trace of every transaction.

    Args:
        df_txs_lx (pd.DataFrame): transactions with at least 'hash', 'timeStamp' and
            'blockNumber' columns.
        node_url (str): URL of an archive node with Geth-style debug functionality.
        ordering (str): ORDERING_PAPER (default; the only mode for published
            artifacts) or ORDERING_EXACT (validation only). See module docstring.
        max_attempts (int): retry budget per trace request (passed to json_retriever).
        workers (int): concurrent trace requests (1 = sequential). The result does
            not depend on it.

    Returns:
        pd.DataFrame: one row per operation, annotated with tracePos, tracePosDepth,
        orderExact, hash, timeStamp and blockNumber.

    Raises:
        RuntimeError: immediately when a transaction's trace cannot be retrieved
        (fail-fast policy: silent gaps must be impossible; with part-file
        checkpointing a rerun resumes cheaply at the failed batch).
    """
    tic = time.time()
    all_records = []
    hashes = [str(h) for h in df_txs_lx['hash']]

    # traces are fetched concurrently (the node does the work; the client mostly
    # waits); map() yields them in input order, so annotation and output are
    # unchanged. On failure, queued requests are cancelled; in-flight ones finish
    # their own retries.
    executor = ThreadPoolExecutor(max_workers=max(1, workers))
    try:
        traces = executor.map(lambda h: json_retriever(h, node_url, max_attempts=max_attempts),
                              hashes)
        for i, (tx_hash, (trace_json_lx, json_flag)) in enumerate(zip(hashes, traces)):
            row = df_txs_lx.iloc[i]
            if not json_flag:
                raise RuntimeError(
                    f"Trace retrieval failed for {tx_hash} "
                    f"(transaction {i + 1} of {len(df_txs_lx)})")
            insert_tracePos(trace_json_lx, ordering=ordering)
            insert_tracePosDepth(trace_json_lx, ordering=ordering)
            records = flatten(trace_json_lx)
            for rec in records:
                rec["hash"] = tx_hash
                # canonical numeric types regardless of the caller's metadata dtypes
                # (a string-typed input column must not leak into the trace dataset)
                rec["timeStamp"] = int(row['timeStamp'])
                rec["blockNumber"] = int(row['blockNumber'])
            all_records.extend(records)
            if (i + 1) % 1000 == 0:
                logger.info(f"TRACE REPLAY: {i + 1} of {len(df_txs_lx)} transactions")
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    df_trace_lx = pd.DataFrame(all_records)
    logger.debug(f"SPEEDTEST: Time to recompute and transform transaction traces: {time.time() - tic}")
    logger.info(f"Number of operations for this level: {len(df_trace_lx)}")
    return df_trace_lx
