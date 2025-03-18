import os
import json
import pickle
import pandas as pd
import logging
import sys

# Add project root to path for absolute imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from src.trace_based_logging.log_construction import transformation_augur_utils
from src.trace_based_logging.log_construction import address_classification
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

def load_resources(base_contract, min_block, max_block, resources_dir, CONFIG):
    logger.info("Loading resources.")
    creations_path = os.path.join(resources_dir, CONFIG["log_folder"], "decoding", f'creations_{base_contract}_{min_block}_{max_block}.pkl')
    contracts_dapp_path = os.path.join(resources_dir, CONFIG["log_folder"], "extraction", f'contracts_dapp_{base_contract}_{min_block}_{max_block}.pkl')
    creations = pickle.load(open(creations_path, "rb"))
    contracts_dapp = pickle.load(open(contracts_dapp_path, "rb"))
    return creations, contracts_dapp

def get_reverted_transactions(resources_dir, base_contract, min_block, max_block, CONFIG):
    logger.info("Log construction identifying reverted transactions.")
    trace_tree_csv = os.path.join(resources_dir, CONFIG["log_folder"], "extraction", f"df_trace_tree_{base_contract}_{min_block}_{max_block}.csv")
    
    try:
        # Read just the header to get the column names
        header = pd.read_csv(trace_tree_csv, nrows=0)
        if "error" not in header.columns:
            logger.info("No reverted transactions found in the extracted data.")
            return set()
        
        # Now read the necessary columns since 'error' is available
        errors = pd.read_csv(trace_tree_csv, usecols=["error", "hash"],
                             dtype={"hash": str, "error": str})
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
        return txs_reverted
    except Exception as e:
        logger.error(f"Error identifying reverted transactions: {e}")
        return set()


def load_if_not_found_in_state(resources_dir, file_name_snipped, state, CONFIG):
    
    base_contract = state["base_contract"]
    csv_path = os.path.join(resources_dir, CONFIG["log_folder"], "decoding", f"{file_name_snipped}_{base_contract}_{CONFIG['min_block']}_{CONFIG['max_block']}.csv")
    pkl_path = os.path.join(resources_dir, CONFIG["log_folder"], "decoding", f"{file_name_snipped}_{base_contract}_{CONFIG['min_block']}_{CONFIG['max_block']}.pkl")

    # Try to load the DataFrame
    if os.path.exists(pkl_path):
        data = pd.read_pickle(pkl_path)
        logger.info(f"Loaded {file_name_snipped} from pickle file.")
    elif os.path.exists(csv_path):
        data = pd.read_csv(csv_path)
        logger.info(f"Loaded {file_name_snipped} from CSV file.")
    else:
        logger.error(f"Neither the pickle file nor the CSV file exists for {file_name_snipped}.")
    return data



############################### ---- DAPP CATEGORIES ---- ###############################

def transform_events_dapp(state, resources_dir, CONFIG,
                          mappings, creations, contracts_dapp, txs_reverted,
                          sensitive_events=False):
    logger.info("Transforming EVENTS DAPP.")

    file_name_snipped = "dapp_events_decoded"

    if file_name_snipped in state:
        events = state[file_name_snipped]
    if file_name_snipped not in state or not isinstance(state[file_name_snipped], pd.DataFrame):
        logger.info("The function input df_log must be a pandas DataFrame. Trying to load the DataFrame from a pickle or CSV file.")
        events = load_if_not_found_in_state(resources_dir, file_name_snipped, state, CONFIG)
        
    events = transformation_augur_utils.initial_transformation_events(events, True, txs_reverted)
    events = transformation_augur_utils.rename_attribute(events, "Activity", "Activity", mappings["event_map_dapp"])
    events = transformation_augur_utils.label_contracts(events, mappings, creations, contracts_dapp)
    events = transformation_augur_utils.rename_attribute(events, "tokenType", "tokenType_name", mappings["token_map"])
    if sensitive_events:
        activity_split_candidates = ["transfer tokens", "give approval to transfer tokens", "mint tokens", "burn tokens"]
        events = transformation_augur_utils.create_contract_sensitive_events(events, mappings, creations, contracts_dapp, activity_split_candidates)
        events = transformation_augur_utils.combine_attributes(events, "Activity", "tokenType_name", "Activity_token_sensitive", ", token type: ", [])
        # Propagate extra info and market type
        market_info = transformation_augur_utils.propagate_extraInfo(events)
        events = pd.merge(events, market_info, on='market', how='left')
        market_type_info = transformation_augur_utils.propagate_marketType(events)
        events = pd.merge(events, market_type_info, on='market', how='left', suffixes=('', '_propagated'))
    else:
        logger.info("Sensitive events not created for EVENTS DAPP.")
    logger.info(f"Number of EVENTS DAPP: {len(events)}")
    return events

def transform_calls_dapp(state, resources_dir, CONFIG,
                         mappings, creations, contracts_dapp, txs_reverted,
                         sensitive_events, market_info=None, market_type_info=None):
    logger.info("Transforming CALLS DAPP.")
    
    file_name_snipped = "dapp_calls_decoded"

    if file_name_snipped in state:
        calls = state[file_name_snipped]
    if file_name_snipped not in state or not isinstance(state[file_name_snipped], pd.DataFrame):
        logger.info("The function input df_log must be a pandas DataFrame. Trying to load the DataFrame from a pickle or CSV file.")
        calls = load_if_not_found_in_state(resources_dir, file_name_snipped, state, CONFIG)

    calls = transformation_augur_utils.initial_transformation_calls(calls, True, txs_reverted)
    calls = transformation_augur_utils.rename_attribute(calls, "Activity", "Activity", mappings["calls_map_dapp"])
    calls = transformation_augur_utils.label_contracts(calls, mappings, creations, contracts_dapp)

    for col in ["orderId", "betterOrderId", "worseOrderId", "tradeGroupId"]:
        if col in calls.columns:
            calls[col] = calls[col].apply(lambda x: "0x" + x.hex() if pd.notnull(x) and isinstance(x, bytes) else str(x) if pd.notnull(x) else pd.NA)
    
    if sensitive_events:
        activity_split_candidates = ["call and transfer Ether", "call trade with limit"]
        calls = transformation_augur_utils.create_contract_sensitive_events(calls, mappings, creations, contracts_dapp, activity_split_candidates)
        if market_info is not None and market_type_info is not None:
            calls = pd.merge(calls, market_info, on='market', how='left')
            calls = pd.merge(calls, market_type_info, on='market', how='left', suffixes=('', '_propagated'))
    else:
        logger.info("Sensitive events not created for CALLS DAPP.")
    logger.info(f"Number of CALLS DAPP: {len(calls)}")
    return calls

def transform_delegatecalls_dapp(state, resources_dir, CONFIG,
                                 mappings, creations, contracts_dapp, txs_reverted,
                                 sensitive_events, market_info=None, market_type_info=None):
    logger.info("Transforming DELEGATECALLS DAPP.")
    
    file_name_snipped = "dapp_delegatecalls_decoded"

    if file_name_snipped in state:
        dcalls = state[file_name_snipped]
    if file_name_snipped not in state or not isinstance(state[file_name_snipped], pd.DataFrame):
        logger.info(f"The function input {file_name_snipped} must be a pandas DataFrame. Trying to load the DataFrame from a pickle or CSV file.")
        dcalls = load_if_not_found_in_state(resources_dir, file_name_snipped, state, CONFIG)
        
    dcalls = transformation_augur_utils.initial_transformation_calls(dcalls, True, txs_reverted)
    dcalls = transformation_augur_utils.rename_attribute(dcalls, "Activity", "Activity", mappings["delegatecalls_map_dapp"])
    dcalls = transformation_augur_utils.label_contracts(dcalls, mappings, creations, contracts_dapp)
    for col in ["orderId", "betterOrderId", "worseOrderId", "tradeGroupId"]:
        if col in dcalls.columns: 
            dcalls[col] = dcalls[col].apply(lambda x: "0x" + x.hex() if pd.notnull(x) and isinstance(x, bytes) else str(x) if pd.notnull(x) else pd.NA)
    if sensitive_events:
        activity_split_candidates = [
            'delegate call to get REP token', 'delegate call to approve', 'delegate call to get universe', 
            'delegate call to get fee window', 'delegate call to get stake', 'delegate call to get payout distribution hash', 
            'delegate call to initialize', 'delegate call to get balance', 'delegate call to transfer token on behalf of the owner', 
            'delegate call to get total supply', 'delegate call to get market', 'delegate call to get outcome', 
            'delegate call to get allowance', 'delegate call to transfer token', 'delegate call to check for container for share token', 
            'delegate call to check if container container for reporting participant', 'delegate call and transfer Ether', 
            'delegate call to redeem', 'delegate call to contribute', 'delegate call to get size', 'delegate call to migrate', 
            'delegate call to check validity', 'delegate call to deposit Ether', 'delegate call to check if designated reporter was correct', 
            'delegate call to check if designated reporter showed', 'delegate call to get payout numerator', 'delegate call to liquidate losing'
        ]
        dcalls = transformation_augur_utils.create_contract_sensitive_events(dcalls, mappings, creations, contracts_dapp, activity_split_candidates)
        if market_info is not None and market_type_info is not None:
            dcalls = pd.merge(dcalls, market_info, on='market', how='left')
            dcalls = pd.merge(dcalls, market_type_info, on='market', how='left', suffixes=('', '_propagated'))
    else:
        logger.info("Sensitive events not created for DELEGATECALLS DAPP.")
    logger.info(f"Number of DELEGATECALLS DAPP: {len(dcalls)}")
    return dcalls

def transform_zero_value_calls_dapp(state, resources_dir, CONFIG,
                                    mappings, creations, contracts_dapp, txs_reverted,
                                    sensitive_events, market_info=None, market_type_info=None):
    
    logger.info("Transforming ZERO VALUE CALLS DAPP.")
    
    file_name_snipped = "dapp_zero_value_calls_decoded"    
    
    if file_name_snipped in state:
        zcalls = state[file_name_snipped]
    if file_name_snipped not in state or not isinstance(state[file_name_snipped], pd.DataFrame):
        logger.info(f"The function input {file_name_snipped} must be a pandas DataFrame. Trying to load the DataFrame from a pickle or CSV file.")
        zcalls = load_if_not_found_in_state(resources_dir, file_name_snipped, state, CONFIG)
        
    zcalls.reset_index(drop=True, inplace=True)
    zcalls = transformation_augur_utils.initial_transformation_calls(zcalls, True, txs_reverted)
    zcalls = transformation_augur_utils.rename_attribute(zcalls, "Activity", "Activity", mappings["calls_zero_value_map_dapp"])
    zcalls = transformation_augur_utils.label_contracts(zcalls, mappings, creations, contracts_dapp)
    for col in ["orderId", "betterOrderId", "worseOrderId", "tradeGroupId"]:
        if col in zcalls.columns:
            zcalls[col] = zcalls[col].apply(lambda x: "0x" + x.hex() if pd.notnull(x) and isinstance(x, bytes) else str(x) if pd.notnull(x) else pd.NA)
    if sensitive_events:
        activity_split_candidates = [
            "call to set controller", "call to initialize crowdsourcer", "call to check if initialization happened",
            "call to initialize", "call to create", "call to get dispute round duration in seconds", "call to check if fork happened",
            "call to get REP token", "call to create REP token", "call to get parent universe", "call to log REP token transferred",
            "call to log fee window creation", "call to get timestamp", "call to create fee token", "call to create map",
            "call to create share token", "call to check if known universe", "call to check if container market",
            "call to create order", "call for trusted transfer", "call to find bounding orders", "call to log order creation",
            "call to fill order", "call to buy complete sets", "call to log order filling", "call to increment open interest",
            "call to log share tokens transfer", "call to log share token minted", "call to check if container for share token",
            "call to check if container for fee window", "call to cancel order", "call to lor order cancellation",
            "call to fill best order", "call to get or cache designated reporter stake", "call to get or create next fee window",
            "call to check if container for reporting participant", "call to log fee token minted", "call to check if container for fee token",
            "call to initiate a trade", "call to sell complete sets", "call to decrement open interest",
            "call to get or cache reporting fee divisor", "call to get REP price in Atto ETH", "call to log share token burned",
            "call to buy participation token", "call to trade with limit", "call to fill best order with limit",
            "call to sell complete sets (public)", "call to log complete sets sold", "call to create dispute crowdsourcer",
            "call to notify about dispute crowdsourcer creation", "call to log dispute crowdsourcer contribution",
            "call to get dispute threshold for fork", "call to log dispute crowdsourcer completion",
            "call to log dispute crowdsourcer tokens minted", "call to get forking market", "call to decrement open interest from market",
            "call to log market finalization", "call to get or create fee window before", "call to log fee token burning",
            "call to claim trading proceeds", "call to log trading proceeds claim", "call to log dispute crowdsourcer tokens burned",
            "call to log dispute crowdsourcer redemption", "call to log complete set purchase", "call to log dispute crowdsourcer tokens transfer",
            "call to contribute", "call to check if disputed", "call to get REP", "call to dispute", "call to withdraw proceeds",
            "call to check if finalized", "call to get dispute token address", "call to approve manager to spend dispute tokens",
            "call to finalize", "call to withdraw fees", "call to get contract fee receiver", "call fee receiver",
            "call to withdraw contribution", "call to get total contribution", "call to get total fees offered",
            "call to buy complete sets with cash (public)", "call to buy (public)", "call to transfer ownership",
            "call to set REP price in Atto ETH", "call to approve"
        ]
        zcalls = transformation_augur_utils.create_contract_sensitive_events(zcalls, mappings, creations, contracts_dapp, activity_split_candidates)
        if market_info is not None and market_type_info is not None:
            zcalls = pd.merge(zcalls, market_info, on='market', how='left')
            zcalls = pd.merge(zcalls, market_type_info, on='market', how='left', suffixes=('', '_propagated'))
        # Optionally adjust the Activity field:
        zcalls["Activity_raw"] = zcalls["Activity"]
        mask_token = ~zcalls["Activity_token_sensitive"].isnull()
        zcalls.loc[mask_token, "Activity"] = zcalls.loc[mask_token, "Activity_token_sensitive"]
        mask_contract = ~zcalls["Activity_contract_sensitive"].isnull()
        zcalls.loc[mask_contract, "Activity"] = zcalls.loc[mask_contract, "Activity_contract_sensitive"]
    else:
        logger.info("Sensitive events not created for ZERO VALUE CALLS DAPP.")
    if "functionName" in zcalls.columns:
        logger.info("Deleting column 'functionName'")
        zcalls.drop("functionName", axis=1, inplace=True)
    logger.info(f"Number of ZERO VALUE CALLS DAPP: {len(zcalls)}")
    return zcalls

def transform_creations_dapp(creations, contracts_dapp):
    logger.info("Transforming CREATIONS DAPP.")
    creations_dapp = creations[creations["to"].isin(contracts_dapp)].copy()
    creations_dapp.reset_index(drop=True, inplace=True)
    creations_dapp["dapp"] = True
    creations_dapp["Activity"] = "create new contract"
    creations_dapp["txHash"] = creations_dapp["hash"]
    creations_dapp.drop("hash", axis=1, inplace=True)
    logger.info(f"Number of CREATIONS DAPP: {len(creations_dapp)}")
    return creations_dapp

############################### ---- NON-DAPP CATEGORIES ---- ###############################

def transform_events_non_dapp(state, resources_dir, CONFIG,
                              mappings, txs_reverted):
    logger.info("Transforming EVENTS NON-DAPP.")
    
    file_name_snipped = "non_dapp_events_decoded"    
    
    if file_name_snipped in state:
        events = state[file_name_snipped]
    if file_name_snipped not in state or not isinstance(state[file_name_snipped], pd.DataFrame):
        logger.info(f"The function input {file_name_snipped} must be a pandas DataFrame. Trying to load the DataFrame from a pickle or CSV file.")
        events = load_if_not_found_in_state(resources_dir, file_name_snipped, state, CONFIG)
    
    events = transformation_augur_utils.initial_transformation_events(events, False, txs_reverted)
    events = transformation_augur_utils.rename_attribute(events, "Activity", "Activity", mappings["event_map_non_dapp"])
    # Additional non-dapp processing could be applied here
    logger.info(f"Number of EVENTS NON-DAPP: {len(events)}")
    return events

def transform_calls_non_dapp(state, resources_dir, CONFIG,
                             mappings, txs_reverted):
    logger.info("Transforming CALLS NON-DAPP.")
    
    file_name_snipped = "non_dapp_calls_decoded"    
    
    if file_name_snipped in state:
        calls = state[file_name_snipped]
    if file_name_snipped not in state or not isinstance(state[file_name_snipped], pd.DataFrame):
        logger.info(f"The function input {file_name_snipped} must be a pandas DataFrame. Trying to load the DataFrame from a pickle or CSV file.")
        calls = load_if_not_found_in_state(resources_dir, file_name_snipped, state, CONFIG)
    
    calls = transformation_augur_utils.initial_transformation_calls(calls, False, txs_reverted)
    calls = transformation_augur_utils.rename_attribute(calls, "Activity", "Activity", mappings["calls_map_non_dapp"])
    # Additional processing if needed
    logger.info(f"Number of CALLS NON-DAPP: {len(calls)}")
    return calls

def transform_delegatecalls_non_dapp(state, resources_dir, CONFIG,
                                     mappings, txs_reverted):
    logger.info("Transforming DELEGATECALLS NON-DAPP.")
    
    file_name_snipped = "non_dapp_delegatecalls_decoded"    
    
    if file_name_snipped in state:
        dcalls = state[file_name_snipped]
    if file_name_snipped not in state or not isinstance(state[file_name_snipped], pd.DataFrame):
        logger.info(f"The function input {file_name_snipped} must be a pandas DataFrame. Trying to load the DataFrame from a pickle or CSV file.")
        dcalls = load_if_not_found_in_state(resources_dir, file_name_snipped, state, CONFIG)
        
    dcalls = transformation_augur_utils.initial_transformation_calls(dcalls, False, txs_reverted)
    dcalls = transformation_augur_utils.rename_attribute(dcalls, "Activity", "Activity", mappings["delegatecalls_map_non_dapp"])
    logger.info(f"Number of DELEGATECALLS NON-DAPP: {len(dcalls)}")
    return dcalls

def transform_zero_value_calls_non_dapp(state, resources_dir, CONFIG,
                                        mappings, txs_reverted):
    logger.info("Transforming ZERO VALUE CALLS NON-DAPP.")
    
    file_name_snipped = "non_dapp_zero_value_calls_decoded"    
    
    if file_name_snipped in state:
        zcalls = state[file_name_snipped]
    if file_name_snipped not in state or not isinstance(state[file_name_snipped], pd.DataFrame):
        logger.info(f"The function input {file_name_snipped} must be a pandas DataFrame. Trying to load the DataFrame from a pickle or CSV file.")
        zcalls = load_if_not_found_in_state(resources_dir, file_name_snipped, state, CONFIG)
        
    zcalls = transformation_augur_utils.initial_transformation_calls(zcalls, False, txs_reverted)
    zcalls = transformation_augur_utils.rename_attribute(zcalls, "Activity", "Activity", mappings["calls_zero_value_map_non_dapp"])
    if "functionName" in zcalls.columns:
        zcalls.drop("functionName", axis=1, inplace=True)
    logger.info(f"Number of ZERO VALUE CALLS NON-DAPP: {len(zcalls)}")
    return zcalls

#def transform_zero_value_delegatecalls_non_dapp(resources_dir, base_contract, min_block, max_block,
#                                                mappings, creations, contracts_dapp, txs_reverted):
#    logger.info("Transforming ZERO VALUE DELEGATECALLS NON-DAPP.")
#    path = os.path.join(resources_dir, f"df_delegatecall_non_dapp_zero_value_{base_contract}_{min_block}_{max_block}.pkl")
#    zdcalls = pickle.load(open(path, "rb"))
#    zdcalls = transformation_augur_utils.initial_transformation_calls(zdcalls, False, txs_reverted)
#    zdcalls = transformation_augur_utils.rename_attribute(zdcalls, "Activity", "Activity", mappings["delegatecalls_zero_value_map_non_dapp"])
#    logger.info(f"Number of ZERO VALUE DELEGATECALLS NON-DAPP: {len(zdcalls)}")
#    return zdcalls

def transform_creations_non_dapp(creations, contracts_dapp):
    logger.info("Transforming CREATIONS NON-DAPP.")
    creations_non_dapp = creations[~creations["to"].isin(contracts_dapp)].copy()
    creations_non_dapp.reset_index(drop=True, inplace=True)
    creations_non_dapp["dapp"] = False
    creations_non_dapp["Activity"] = "create new contract"
    creations_non_dapp["txHash"] = creations_non_dapp["hash"]
    creations_non_dapp.drop("hash", axis=1, inplace=True)
    logger.info(f"Number of CREATIONS NON-DAPP: {len(creations_non_dapp)}")
    return creations_non_dapp

def save_transformed_category(df, category_name, base_contract, min_block, max_block, resources_dir, log_folder):
    file_base = f"{category_name}_{base_contract}_{min_block}_{max_block}"
    csv_path = os.path.join(resources_dir, log_folder, "transformation", file_base + ".csv")
    pkl_path = os.path.join(resources_dir, log_folder, "transformation", file_base + ".pkl")
    df.to_csv(csv_path)
    with open(pkl_path, 'wb') as f:
        pickle.dump(df, f)
    logger.info(f"Saved {category_name} to {csv_path} and {pkl_path}")

def transform_augur_data(resources_dir, log_folder, state, CONFIG):
    """
    CONFIG is a dictionary that controls whether to process each category.
    Expected keys:
      - events_dapp, calls_dapp, delegatecalls_dapp, zero_value_calls_dapp, creations_dapp,
      - events_non_dapp, calls_non_dapp, delegatecalls_non_dapp, zero_value_calls_non_dapp,
      - zero_value_delegatecalls_non_dapp, creations_non_dapp,
      - sensitive_events (applies to all sensitive processing)
    """
    logger.info("Data transformation to prepare the event log started.")    
    
    min_block = CONFIG["min_block"]
    max_block = CONFIG["max_block"]
    
    base_contract = state["base_contract"]
    
    mapping_path = os.path.join(os.path.dirname(__file__), "mappings.json")
    mappings = load_mappings(mapping_path)
    creations, contracts_dapp = load_resources(base_contract, min_block, max_block, resources_dir, CONFIG)
    txs_reverted = get_reverted_transactions(resources_dir, base_contract, min_block, max_block, CONFIG)
    
    market_info = None
    market_type_info = None
    
    ############################### ---- DAPP ---- ###############################
    
    # Process EVENTS DAPP
    if CONFIG.get("dapp_events", False):
        events_dapp = transform_events_dapp(state, resources_dir, CONFIG,
                                             mappings, creations, contracts_dapp, txs_reverted,
                                             sensitive_events=CONFIG.get("sensitive_events", False))
        save_transformed_category(events_dapp, "dapp_events", base_contract, min_block, max_block, resources_dir, log_folder)
        if CONFIG.get("sensitive_events", False):
            market_info = transformation_augur_utils.propagate_extraInfo(events_dapp)
            market_type_info = transformation_augur_utils.propagate_marketType(events_dapp)
    else:
        logger.info("Skipping EVENTS DAPP.")
    
    # Process CALLS DAPP
    if CONFIG.get("dapp_calls", False):
        calls_dapp = transform_calls_dapp(state, resources_dir, CONFIG,
                                          mappings, creations, contracts_dapp, txs_reverted,
                                          sensitive_events=CONFIG.get("sensitive_events", False),
                                          market_info=market_info, market_type_info=market_type_info)
        save_transformed_category(calls_dapp, "dapp_calls", base_contract, min_block, max_block, resources_dir, log_folder)
    else:
        logger.info("Skipping CALLS DAPP.")
    
    # Process DELEGATECALLS DAPP
    if CONFIG.get("dapp_delegatecalls", False):
        dcalls_dapp = transform_delegatecalls_dapp(state, resources_dir, CONFIG,
                                                    mappings, creations, contracts_dapp, txs_reverted,
                                                    sensitive_events=CONFIG.get("sensitive_events", False),
                                                    market_info=market_info, market_type_info=market_type_info)
        save_transformed_category(dcalls_dapp, "dapp_delegatecalls", base_contract, min_block, max_block, resources_dir, log_folder)
    else:
        logger.info("Skipping DELEGATECALLS DAPP.")
    
    # Process ZERO VALUE CALLS DAPP
    if CONFIG.get("dapp_zero_value_calls", False):
        zcalls_dapp = transform_zero_value_calls_dapp(state, resources_dir, CONFIG,
                                                       mappings, creations, contracts_dapp, txs_reverted,
                                                       sensitive_events=CONFIG.get("sensitive_events", False),
                                                       market_info=market_info, market_type_info=market_type_info)
        save_transformed_category(zcalls_dapp, "dapp_zero_value_calls", base_contract, min_block, max_block, resources_dir, log_folder)
    else:
        logger.info("Skipping ZERO VALUE CALLS DAPP.")
    
    # Process CREATIONS DAPP
    if CONFIG.get("dapp_creations", False):
        creations_dapp = transform_creations_dapp(creations, contracts_dapp)
        save_transformed_category(creations_dapp, "dapp_creations", base_contract, min_block, max_block, resources_dir, log_folder)
    else:
        logger.info("Skipping CREATIONS DAPP.")
    
    ############################### ---- NON-DAPP ---- ###############################
    
    if CONFIG.get("non_dapp_events", False):
        events_non_dapp = transform_events_non_dapp(state, resources_dir, CONFIG,
                                                     mappings, txs_reverted)
        save_transformed_category(events_non_dapp, "non_dapp_events", base_contract, min_block, max_block, resources_dir, log_folder)
    else:
        logger.info("Skipping EVENTS NON-DAPP.")
    
    if CONFIG.get("non_dapp_calls", False):
        calls_non_dapp = transform_calls_non_dapp(state, resources_dir, CONFIG,
                                                   mappings, txs_reverted)
        save_transformed_category(calls_non_dapp, "non_dapp_calls", base_contract, min_block, max_block, resources_dir, log_folder)
    else:
        logger.info("Skipping CALLS NON-DAPP.")
    
    if CONFIG.get("non_dapp_delegatecalls", False):
        dcalls_non_dapp = transform_delegatecalls_non_dapp(state, resources_dir, CONFIG,
                                                           mappings, txs_reverted)
        save_transformed_category(dcalls_non_dapp, "non_dapp_delegatecalls", base_contract, min_block, max_block, resources_dir, log_folder)
    else:
        logger.info("Skipping DELEGATECALLS NON-DAPP.")
    
    if CONFIG.get("non_dapp_zero_value_calls", False):
        zcalls_non_dapp = transform_zero_value_calls_non_dapp(state, resources_dir, CONFIG,
                                                               mappings, txs_reverted)
        save_transformed_category(zcalls_non_dapp, "non_dapp_zero_value_calls", base_contract, min_block, max_block, resources_dir, log_folder)
    else:
        logger.info("Skipping ZERO VALUE CALLS NON-DAPP.")
    
#    if CONFIG.get("non_dapp_zero_value_delegatecalls", False):
#        zdcalls_non_dapp = transform_zero_value_delegatecalls_non_dapp(resources_dir, base_contract, min_block, max_block,
#                                                                       mappings, creations, contracts_dapp, txs_reverted)
#        save_transformed_category(zdcalls_non_dapp, "non_dapp__zero_value_delegatecalls", base_contract, min_block, max_block, resources_dir, log_folder)
#    else:
#        logger.info("Skipping ZERO VALUE DELEGATECALLS NON-DAPP.")
    
    if CONFIG.get("non_dapp_creations", False):
        creations_non_dapp = transform_creations_non_dapp(creations, contracts_dapp)
        save_transformed_category(creations_non_dapp, "non_dapp_creations", base_contract, min_block, max_block, resources_dir, log_folder)
    else:
        logger.info("Skipping CREATIONS NON-DAPP.")
    
    logger.info("Data transformation to prepare the event log complete.")
    
def build_address_dict(resources_dir, log_folder, base_contract, min_block, max_block, node_url, CONFIG):
    logger.info("Building address dictionary.")        
    mapping_path = os.path.join(os.path.dirname(__file__), "mappings.json")
    mappings = load_mappings(mapping_path)
    creations, contracts_dapp = load_resources(base_contract, min_block, max_block, resources_dir)
    
    address_dict = address_classification.create_address_dict(base_contract, log_folder, resources_dir, min_block, max_block, contracts_dapp, node_url, creations, mappings, CONFIG)
    path = os.path.join(resources_dir, log_folder, "address_dict_" + str(min_block) + "_" + str(max_block) + ".pkl")
    pickle.dump(address_dict, open(path, 'wb'))
    
    logger.info("Building address dictionary complete.")

