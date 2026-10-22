import math
import os

import pandas as pd

from src.trace_based_logging.logging_config import setup_logging
from src.trace_based_logging.config import build_node_url
from src.trace_based_logging import storage
from src.trace_based_logging.raw_trace_retriever import (
    get_transactions, get_receipts, trace_transformation, create_relations)
from src.trace_based_logging.raw_trace_retriever.trace_transformation import (
    ORDERING_PAPER, ORDERING_EXACT)

logger = setup_logging()

# transactions traced per part file (each part is a resumable checkpoint)
BATCH_SIZE = 500


def check_block_range(recorded, config, what):
    """Persisted discovery state may only be reused for the block range it was
    produced for: a mismatch fails fast (a changed range needs a new log_folder);
    state from before ranges were recorded is accepted with a warning."""
    expected = (config["min_block"], config["max_block"])
    if recorded is None:
        logger.warning(f"{what} records no block range (older run); assuming "
                       f"{expected[0]}-{expected[1]}.")
    elif tuple(recorded) != expected:
        raise RuntimeError(
            f"{what} was produced for blocks {recorded[0]}-{recorded[1]}, but the "
            f"configured range is {expected[0]}-{expected[1]}. Use a new log_folder "
            f"for a different block range.")


def creation_blocks(trace_tree, contracts):
    """address -> block of its earliest CREATE/CREATE2 in the trace tree, for the
    given contracts (lowercased). Contracts without a traced creation (the
    configured ones) are absent. Earliest over all creations, reverted ones
    included: a lower bound is always safe for the event search."""
    if trace_tree is None or trace_tree.empty:
        return {}
    creates = trace_tree[trace_tree["type"].isin(["CREATE", "CREATE2"])]
    created = creates["to"].astype(str).str.lower()
    wanted = created.isin({c.lower() for c in contracts})
    first = (pd.to_numeric(creates.loc[wanted, "blockNumber"])
             .groupby(created[wanted]).min())
    return {address: int(block) for address, block in first.items()}


def fetch_transactions(config, contracts_lx, start_blocks=None, level_prefix=None):
    """Fetch candidate transactions for the given contracts from the configured
    sources (Etherscan normal/internal, node events — the paper's three sources).

    start_blocks (address -> creation block) narrows the event search per contract;
    the Etherscan sources look addresses up in an index, where the range costs
    nothing, so they keep the configured range.

    level_prefix (e.g. <extraction_dir>/transactions_level-03): each source's result
    is persisted as <level_prefix>.<source>.parquet the moment it is complete (with
    its block range) and reused on a rerun, so a failure in one source does not
    cost the others again. The combined result is the same either way.

    Fail-fast: source errors propagate instead of being logged away (a silently
    missing source would make the discovery incomplete without any trace of it).
    """
    def source(name, fetch):
        path = f"{level_prefix}.{name}.parquet" if level_prefix else None
        if path and os.path.exists(path):
            check_block_range(storage.parquet_block_range(path), config, path)
            df = pd.read_parquet(path)
            logger.info(f"{name.upper()} transactions: reusing persisted result "
                        f"({len(df)} rows) from {path}")
            return df
        df = fetch()
        if path:
            storage.write_atomic_parquet(
                path, df if not df.empty else pd.DataFrame(columns=["hash"]),
                block_range=(config["min_block"], config["max_block"]))
            logger.info(f"{name.upper()} transactions: {len(df)} rows saved to {path}")
        return df

    frames = []
    if config["extract_normal_transactions"]:
        frames.append(source("normal", lambda: get_transactions.get_transactions(
            contracts_lx, config["min_block"], config["max_block"],
            internal_flag="normal", etherscan_api_key=config["etherscan_api_key"])))
    if config["extract_internal_transactions"]:
        frames.append(source("internal", lambda: get_transactions.get_transactions(
            contracts_lx, config["min_block"], config["max_block"],
            internal_flag="internal", etherscan_api_key=config["etherscan_api_key"])))
    if config["extract_transactions_by_events"]:
        frames.append(source("events", lambda: get_transactions.get_transactions_by_events(
            build_node_url(config), contracts_lx, config["min_block"], config["max_block"],
            start_blocks=start_blocks)))

    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=["hash"])
    transactions = pd.concat(frames, ignore_index=True)
    # the sources type these differently (Etherscan: str, node event logs: int);
    # normalize here so the persisted level file has stable arrow types
    for col in ("blockNumber", "timeStamp"):
        if col in transactions.columns:
            transactions[col] = pd.to_numeric(transactions[col]).astype("Int64")
    transactions.drop_duplicates(subset="hash", keep="last", inplace=True)
    transactions.reset_index(drop=True, inplace=True)
    return transactions


def process_transactions(config, state, extraction_dir=None, batch_size=BATCH_SIZE):
    """Level-wise discovery loop (Figure 4 of the paper): fetch transactions for the
    current contract frontier, replay their traces, grow the DApp contract set along
    CREATE relations, repeat until no new contracts are found.

    When extraction_dir is given, the run is persistent and resumable:
    - each discovery source's result is written as soon as it is complete
      (transactions_level-NN.<source>.parquet), so a failing source does not cost
      the finished ones again;
    - each level's transaction list is written before tracing starts;
    - traces are written as part files per batch (storage.write_part), so an
      interrupted level resumes at its first missing part;
    - the discovery frontier is checkpointed after every completed level and loaded
      back automatically on the next invocation.
    Checkpoint, level lists and source files record the block range; reusing them
    under a different configured range fails fast.

    The ordering mode comes from config["ordering"]; the default, used for the
    published logs, is the paper's enumeration (Listing 2).
    """
    node_url = build_node_url(config)
    ordering = config.get("ordering", ORDERING_PAPER)
    if ordering == ORDERING_EXACT:
        logger.warning("ordering='exact' is for validation; the published logs use the "
                       "paper's enumeration.")

    if extraction_dir:
        checkpoint = storage.load_checkpoint(extraction_dir)
        if checkpoint:
            check_block_range(checkpoint.pop("block_range", None), config,
                              os.path.join(extraction_dir, storage.CHECKPOINT_JSON))
            state.update(checkpoint)
            logger.info(f"Resuming extraction from checkpoint: level {state['level']}, "
                        f"{len(state['contracts_dapp'])} DApp contracts, "
                        f"{len(state['all_transactions'])} transactions seen.")
            # rebuild the in-memory trace tree from the persisted level parts:
            # a resumed run may find no new transactions at all and still must
            # hand the traces to the downstream stages
            if state.get("trace_tree") is None:
                frames = [storage.read_dataset(extraction_dir,
                                               f"trace_tree/level-{lvl:02d}")
                          for lvl in range(1, state["level"])]
                frames = [f for f in frames if not f.empty]
                if frames:
                    state["trace_tree"] = pd.concat(frames, ignore_index=True)
                    logger.info(f"Restored trace tree from persisted parts: "
                                f"{len(state['trace_tree'])} operations.")

    while state["contracts_lx"]:
        level = state["level"]
        logger.info(f"##### LEVEL {level}: GETTING TRANSACTIONS #####")

        level_tx_path = (os.path.join(extraction_dir, f"transactions_level-{level:02d}.parquet")
                         if extraction_dir else None)
        if level_tx_path and os.path.exists(level_tx_path):
            check_block_range(storage.parquet_block_range(level_tx_path), config,
                              level_tx_path)
            transactions = pd.read_parquet(level_tx_path)
            logger.info(f"Level {level}: reusing persisted transaction list "
                        f"({len(transactions)} transactions).")
        else:
            transactions = fetch_transactions(
                config, state["contracts_lx"],
                start_blocks=creation_blocks(state["trace_tree"], state["contracts_lx"]),
                level_prefix=(level_tx_path[:-len(".parquet")] if level_tx_path else None))
            transactions = transactions[~transactions["hash"].isin(state["all_transactions"])]
            transactions.reset_index(drop=True, inplace=True)
            if level_tx_path and not transactions.empty:
                storage.write_atomic_parquet(
                    level_tx_path, transactions,
                    block_range=(config["min_block"], config["max_block"]))

        if transactions.empty:
            logger.info("No additional transactions were found. Extraction ends.")
            break
        logger.info(f"New transactions at level {level}: {len(transactions)}")

        logger.info(f"##### LEVEL {level}: COMPUTING TRACES #####")
        dataset = f"trace_tree/level-{level:02d}"
        done = set(storage.completed_parts(extraction_dir, dataset)) if extraction_dir else set()
        n_batches = math.ceil(len(transactions) / batch_size)
        level_frames = []
        for b in range(n_batches):
            part_index = b + 1
            chunk = transactions.iloc[b * batch_size:(b + 1) * batch_size]
            if part_index in done:
                level_frames.append(storage.read_part(extraction_dir, dataset, part_index))
                continue
            traces = trace_transformation.tx_to_trace(chunk, node_url, ordering=ordering)
            if extraction_dir:
                storage.write_part(extraction_dir, dataset, traces, part_index)
            level_frames.append(traces)
            if n_batches > 1:
                logger.info(f"Level {level}: traced batch {part_index}/{n_batches}")
        level_traces = pd.concat(level_frames, ignore_index=True)

        if state["trace_tree"] is None:
            state["trace_tree"] = level_traces
        else:
            state["trace_tree"] = pd.concat([state["trace_tree"], level_traces],
                                            ignore_index=True)
        state["all_transactions"].update(transactions["hash"].tolist())

        logger.info("Identifying relevant CREATE-relations.")
        state["contracts_dapp"], state["contracts_lx"] = create_relations.create_relations(
            state["trace_tree"], state["contracts_dapp"], state["contracts_lx"],
            state["contracts_non_dapp"])
        logger.info(f"New contracts at level {level}: {len(state['contracts_lx'])}")

        state["level"] = level + 1
        if extraction_dir:
            storage.save_checkpoint(extraction_dir, state,
                                    block_range=(config["min_block"], config["max_block"]))

    logger.info(f"Total extracted operations: "
                f"{0 if state['trace_tree'] is None else len(state['trace_tree'])}")


def insert_transaction_index(config, state, build_node_url_func, cache_path=None,
                             extraction_dir=None):
    """Annotate the trace operations with transactionIndex from the transaction
    receipts (Section 7 of the paper, footnote 21: web3 get_transaction_receipt).

    Uses get_receipts, one receipt per discovered transaction, which also yields the
    revert status and the log count; the receipts table is written next to the
    extraction output for the later stages. Any unresolved receipt raises inside
    get_receipts.
    """
    hash_list = state["trace_tree"]["hash"].unique()
    receipts = get_receipts.get_receipts(hash_list, build_node_url_func(config),
                                         cache_path=cache_path)
    if extraction_dir:
        storage.write_atomic_parquet(os.path.join(extraction_dir, "receipts.parquet"),
                                     receipts)
    state["receipts"] = receipts
    index_map = receipts.set_index("txHash")["transactionIndex"]
    state["trace_tree"]["transactionIndex"] = (
        state["trace_tree"]["hash"].map(index_map).astype("int64"))
