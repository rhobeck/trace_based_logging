"""Entry point of the pipeline: runs the stages extraction, decoding and
transformation as enabled in config.json, each stage checkpointed so that a rerun
resumes where it stopped.

Usage: python src/trace_based_logging/__main__.py
"""
import os
import sys
import pandas as pd
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import pickle
from src.trace_based_logging.logging_config import setup_logging

logger = setup_logging()

from src.trace_based_logging.config import load_config, build_node_url, initialize_extraction_state
from src.trace_based_logging.extraction import process_transactions, insert_transaction_index
from src.trace_based_logging.saving import save_trace_data, folder_set_up
from src.trace_based_logging.decoding import decode_all


def main():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    config_path = os.path.join(dir_path, 'config.json')

    # Fail-fast throughout: a failed phase stops the run instead of continuing with
    # broken state (every stage checkpoints, so a rerun resumes cheaply).
    logger.info("STARTING SET-UP PHASE")
    config = load_config(config_path)
    from src.trace_based_logging.raw_trace_retriever import trace_retriever_utils
    state = initialize_extraction_state(config, trace_retriever_utils)
    trace_retriever_utils.check_socket(config["host"], config["port"])
    folder_set_up(dir_path, config)
    extraction_dir = os.path.join(dir_path, "resources", config["log_folder"], "extraction")
    # shared caches (reused across runs; fetch only what is missing): one receipt
    # per transaction from the node, one ABI per address from Etherscan. They
    # live beside the run folders so that a run with another log_folder reuses
    # them; the folder is safe to delete, the next run fetches again.
    cache_dir = os.path.join(dir_path, "resources", "cache")
    receipts_cache = os.path.join(cache_dir, "receipts.parquet")
    abi_cache = os.path.join(cache_dir, "abi_cache.parquet")
    os.makedirs(cache_dir, exist_ok=True)

    if config["extraction"]:
        logger.info("STARTING EXTRACTION PHASE")
        process_transactions(config, state, extraction_dir=extraction_dir)
        insert_transaction_index(config, state, build_node_url,
                                 cache_path=receipts_cache, extraction_dir=extraction_dir)
        save_trace_data(config, state, dir_path)
    else:
        logger.info("Skipping extraction phase.")

    if config["decoding"]:
        logger.info("STARTING DECODING PHASE")
        from src.trace_based_logging.trace_decoder import data_preparation

        if not config["extraction"]:
            state["trace_tree"] = data_preparation.load_data(config, state, "df_trace_tree")
            state["contracts_dapp"] = data_preparation.load_data(config, state, "contracts_dapp")

        # Per-operation revert flag, computed on the full trace tree before the
        # category split so every category inherits it, and audited against the
        # receipts; a mismatch stops the run.
        from src.trace_based_logging.log_construction.transformation_augur import (
            flag_reverted_operations, validate_revert_flags)
        state["trace_tree"] = flag_reverted_operations(state["trace_tree"])
        validate_revert_flags(state["trace_tree"], pd.read_parquet(receipts_cache))

        df_log = data_preparation.base_transformation(state["trace_tree"], state["contracts_dapp"])
        del state["trace_tree"]
        abi_path = os.path.join(dir_path, "resources", config["log_folder"], "decoding",
                                f"dict_abi_{state['base_contract']}_{config['min_block']}_{config['max_block']}.pkl")
        abi_addresses = data_preparation.restrict_to_decoded_categories(
            data_preparation.address_selection(df_log), state["contracts_dapp"], config)
        dict_abi = data_preparation.create_abi_dict(
            abi_addresses, config["etherscan_api_key"], abi_path, cache_path=abi_cache)
        pickle.dump(dict_abi, open(abi_path, 'wb'))
        logger.info(f"Saved ABI dictionary at: {abi_path}")
        decode_all(df_log, state, config, dict_abi, build_node_url)
    else:
        logger.info("Skipping decoding phase.")

    if config["transformation"]:
        logger.info("STARTING TRANSFORMATION PHASE")
        from src.trace_based_logging.log_construction import transformation_augur
        from src.trace_based_logging.log_construction import log_construction_augur
        RESOURCES_DIR = os.path.join(dir_path, "resources")
        LOG_FOLDER = config["log_folder"]
        transformation_augur.transform_augur_data(RESOURCES_DIR, LOG_FOLDER, state, config)
        config["base_contract"] = state["base_contract"]
        del state
        # Account types (Table 3's EOA/CA attribute) are classified from the
        # run's own data inside build_log (node required for getCode).
        # XML-OCEL stays off at full scale (the derived view would be tens of GB);
        # SQLite + the parquet tables are the artifacts of record.
        log_construction_augur.build_log(RESOURCES_DIR, LOG_FOLDER, config,
                                         export_xml=False)
    else:
        logger.info("Skipping transformation phase.")
    print("DONE")


if __name__ == '__main__':
    main()
