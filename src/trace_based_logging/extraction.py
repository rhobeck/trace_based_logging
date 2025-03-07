import pandas as pd
import time
from src.trace_based_logging.logging_config import setup_logging
from src.trace_based_logging.raw_trace_retriever import get_transactions, get_txIndex, trace_transformation, create_relations

logger = setup_logging()

def fetch_transactions(config, contracts_lx):
    transactions = pd.DataFrame()
    # Normal transactions
    if config["extract_normal_transactions"]:
        try:
            transactions_normal = get_transactions.get_transactions(
                contracts_lx, config["min_block"], config["max_block"],
                internal_flag="normal", etherscan_api_key=config["etherscan_api_key"]
            )
            transactions = pd.concat([transactions, transactions_normal])
        except Exception as e:
            logger.error(f"Error fetching normal transactions: {e}")
    # Internal transactions
    if config["extract_internal_transactions"]:
        try:
            transactions_internal = get_transactions.get_transactions(
                contracts_lx, config["min_block"], config["max_block"],
                internal_flag="internal", etherscan_api_key=config["etherscan_api_key"]
            )
            transactions = pd.concat([transactions, transactions_internal])
        except Exception as e:
            logger.error(f"Error fetching internal transactions: {e}")
    # Transactions by events
    if config["extract_transactions_by_events"]:
        try:
            from src.trace_based_logging.config import build_node_url  # reusing our function
            transactions_events = get_transactions.get_transactions_by_events(
                build_node_url(config), contracts_lx, config["min_block"], config["max_block"]
            )
            transactions = pd.concat([transactions, transactions_events])
        except Exception as e:
            logger.error(f"Error fetching transactions by events: {e}")
    # Remove duplicates based on transaction hash
    transactions.drop_duplicates(subset='hash', keep="last", inplace=True)
    return transactions

def process_transactions(config, state):
    from src.trace_based_logging.config import build_node_url
    level = 1
    while state["contracts_lx"]:
        logger.info("##### GETTING TRANSACTIONS #####")
        transactions = fetch_transactions(config, state["contracts_lx"])
        transactions = transactions[~transactions["hash"].isin(state["all_transactions"])]
        if transactions.empty:
            logger.info("No additional transactions were found. Extraction ends.")
            break
        logger.info(f"New transactions for next iteration: {len(transactions)}")
        transactions.reset_index(drop=True, inplace=True)
        state["all_transactions"].update(transactions["hash"].tolist())

        logger.info("##### COMPUTING TRACES #####")
        traces = trace_transformation.tx_to_trace(transactions, build_node_url(config))
        if state["trace_tree"] is None:
            state["trace_tree"] = traces
        else:
            state["trace_tree"] = pd.concat([state["trace_tree"], traces], axis=0)
        logger.info("SUCCESS: Traces computed.")

        logger.info("Identifying relevant CREATE-relations.")
        state["contracts_dapp"], state["contracts_lx"] = create_relations.create_relations(
            state["trace_tree"], state["contracts_dapp"], state["contracts_lx"], state["non_dapp_contracts"]
        )
        logger.info(f"New contracts at level {level}: {len(state['contracts_lx'])}")
        level += 1
        time.sleep(1)
    logger.info(f"Total extracted operations: {len(state['trace_tree'])}")

def insert_transaction_index(config, state, build_node_url_func):
    hash_list = state["trace_tree"]["hash"].unique()
    transaction_indexes = get_txIndex.get_txIndex(hash_list, build_node_url_func(config))
    state["trace_tree"]["transactionIndex"] = state["trace_tree"]["hash"].astype(str).map(transaction_indexes).fillna(state["trace_tree"]["hash"])
