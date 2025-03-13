import os
import json
import pickle
import pandas as pd
import logging

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))


from src.trace_based_logging.log_construction import log_construction_utils

from src.trace_based_logging.logging_config import setup_logging
logger = setup_logging()

def load_mappings(mapping_path):
    try:
        with open(mapping_path, 'r') as file:
            mappings = json.load(file)
        return mappings
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Error loading mappings: {e}")
        raise

def load_resources(base_contract, min_block, max_block, resources_dir):
    # Load creations, contracts_dapp, etc.
    logger.info("Log construction loading resources.")
    creations_path = os.path.join(resources_dir, f'df_creations_{base_contract}_{min_block}_{max_block}.pkl')
    contracts_dapp_path = os.path.join(resources_dir, f'contracts_dapp_{base_contract}_{min_block}_{max_block}.pkl')
    creations = pickle.load(open(creations_path, "rb"))
    contracts_dapp = pickle.load(open(contracts_dapp_path, "rb"))
    return creations, contracts_dapp

def get_reverted_transactions(resources_dir, base_contract, min_block, max_block):
    """
    Reads the trace CSV file, identifies operations with errors
    that indicate a reverted transaction, and returns a set of transaction hashes.
    """
    logger.info("Log construction identifying reverted transactions.")
    trace_tree_csv = os.path.join(resources_dir, f"df_trace_tree_{base_contract}_{min_block}_{max_block}.csv")
    try:
        errors = pd.read_csv(trace_tree_csv, usecols=["error", "hash"], dtype={"hash": str, "error": str})
        errors.reset_index(drop=True, inplace=True)
        mask_reverted = errors["error"].isin([
            'out of gas', 
            'invalid jump destination',
            'execution reverted',
            'write protection',
            'invalid opcode: INVALID',
            'contract creation code storage out of gas'
        ])
        txs_reverted = set(errors.loc[mask_reverted, "hash"])
        logger.info(f"Number of reverted transactions: {len(txs_reverted)}")
        # Free up memory
        del errors
        return txs_reverted
    except Exception as e:
        logger.error(f"Error identifying reverted transactions: {e}")
        return set()

def transform_events(events_path, mappings, creations, contracts_dapp, txs_reverted, resources_dir, sensitive_events=False):
    logger.info("Log construction loading events.")
    events = pickle.load(open(events_path, "rb"))
    logger.info("Log construction transforming events.")
    events = log_construction_utils.initial_transformation_events(events, True, txs_reverted)
    events = log_construction_utils.rename_attribute(events, "Activity", "Activity", mappings["event_map_dapp"])
    events = log_construction_utils.label_contracts(events, mappings, creations, contracts_dapp)
    # Additional sensitive events transformation if enabled
    if sensitive_events:
        events = log_construction_utils.create_contract_sensitive_events(events, mappings, creations, contracts_dapp, [
            "transfer tokens", "give approval to transfer tokens", "mint tokens", "burn tokens"
        ])
        events = log_construction_utils.combine_attributes(events, "Activity", "tokenType_name", "Activity_token_sensitive", ", token type: ", [])
    else:
        logger.info("Sensitive events were not created.")
    return events

def save_transformed_events(events, base_contract, min_block, max_block, resources_dir, log_folder):
    file_name = f"events_dapp_{base_contract}_{min_block}_{max_block}"
    csv_path = os.path.join(resources_dir, log_folder, file_name + ".csv")
    pkl_path = os.path.join(resources_dir, log_folder, file_name + ".pkl")
    events.to_csv(csv_path)
    with open(pkl_path, 'wb') as f:
        pickle.dump(events, f)
    logger.info(f"Saved transformed events to {csv_path} and {pkl_path}")

def build_log(resources_dir, log_folder, base_contract, min_block, max_block, sensitive_events=False):
    # Example orchestration function
    logger.info("Log construction started.")
    mapping_path = os.path.join(os.path.dirname(__file__), "mappings.json")
    mappings = load_mappings(mapping_path)
    creations, contracts_dapp = load_resources(base_contract, min_block, max_block, resources_dir)
    
    # Identify reverted transactions
    txs_reverted = get_reverted_transactions(resources_dir, base_contract, min_block, max_block)

    events_dapp_path = os.path.join(resources_dir, f'df_events_dapp_{base_contract}_{min_block}_{max_block}.pkl')
    
    events_dapp = transform_events(events_dapp_path, mappings, creations, contracts_dapp, txs_reverted, resources_dir, sensitive_events)
    # save_transformed_events(events_dapp, base_contract, min_block, max_block, resources_dir, log_folder)
    logger.info("Log construction complete.")

if __name__ == '__main__':
    RESOURCES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'resources'))
    LOG_FOLDER = "log_120325"
    CONFIG = {
        "base_contract": "0x75228dce4d82566d93068a8d5d49435216551599",
        "min_block": 5926229,
        "max_block": 11229577
    }
    build_log(RESOURCES_DIR, LOG_FOLDER, CONFIG["base_contract"], CONFIG["min_block"], CONFIG["max_block"], sensitive_events=False)