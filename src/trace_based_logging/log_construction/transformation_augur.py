"""The transformation stage of the Augur case study: turns the decoded category
tables into the activity-labelled operation tables (activity names from
mappings.json), flags reverted operations from the trace tree and audits the flags
against the receipts, and writes one table per category and DApp side.

Functions: load_mappings, load_resources, flag_reverted_operations,
validate_revert_flags, transform_<category>_<side>, save_transformed_category,
transform_augur_data.
"""
import os
import json
import pickle
import pandas as pd
import logging
import sys

# Add project root to path for absolute imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from src.trace_based_logging.log_construction import transformation_augur_utils
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
    """Parquet is the canonical form; pickle is read when present."""
    logger.info("Loading resources.")
    creations_base = os.path.join(resources_dir, CONFIG["log_folder"], "decoding",
                                  f'creations_{base_contract}_{min_block}_{max_block}')
    contracts_base = os.path.join(resources_dir, CONFIG["log_folder"], "extraction",
                                  f'contracts_dapp_{base_contract}_{min_block}_{max_block}')
    if os.path.exists(creations_base + ".parquet"):
        creations = pd.read_parquet(creations_base + ".parquet")
    else:
        creations = pickle.load(open(creations_base + ".pkl", "rb"))
    if os.path.exists(contracts_base + ".parquet"):
        contracts_dapp = set(pd.read_parquet(contracts_base + ".parquet")["address"])
    else:
        contracts_dapp = pickle.load(open(contracts_base + ".pkl", "rb"))
    return creations, contracts_dapp

def flag_reverted_operations(df):
    """Per-operation revert flag.

    An operation's state changes were rolled back iff its own frame OR any
    ancestor frame in the call tree errored (the EVM rollback rule); log entries
    inherit the flag of their emitting frame (they share its tracePosDepth
    prefix). Any non-null `error` counts — no error-string whitelist, so client
    vocabulary changes cannot silently unflag operations; correctness is instead
    enforced against receipts by validate_revert_flags (fail-fast).
    """
    df["flag_reverted"] = False
    if "error" not in df.columns:
        return df
    errored = df.loc[df["error"].notna(), ["hash", "tracePosDepth"]]
    if errored.empty:
        return df

    # ancestor paths per transaction, only for transactions containing an
    # errored frame at all
    prefixes = errored.groupby("hash")["tracePosDepth"].agg(tuple).to_dict()
    affected = df["hash"].isin(prefixes.keys())

    def rolled_back(row):
        path = row["tracePosDepth"]
        return any(path == p or path.startswith(p + ".")
                   for p in prefixes[row["hash"]])

    df.loc[affected, "flag_reverted"] = (
        df.loc[affected, ["hash", "tracePosDepth"]].apply(rolled_back, axis=1))
    return df


def validate_revert_flags(df, receipts):
    """Audit of flag_reverted against the receipts, over all transactions.

    Check 1: a transaction's receipt says status=0 iff its root frame is flagged
             (equivalently: iff ALL its operations are flagged).
    Check 2: for every successful transaction, the receipt holds exactly the logs
            of effective frames, so its logCount must equal the transaction's
            count of UNFLAGGED log operations.
    """
    r = receipts.set_index("txHash")
    # the receipts cache is shared across runs and may cover a larger population
    # than this run's trace tree; the audit is full-population FOR THIS RUN, so
    # scope the receipts to the transactions actually present
    r = r.loc[r.index.intersection(df["hash"].unique())]

    root_flag = df.loc[df["tracePosDepth"] == "1"].set_index("hash")["flag_reverted"]
    mismatch = root_flag != (r.loc[root_flag.index, "status"] == 0)
    if mismatch.any():
        raise ValueError(f"Check 1: {int(mismatch.sum())} transactions where "
                         f"flag_reverted contradicts receipt status, e.g. "
                         f"{list(root_flag.index[mismatch][:5])}")

    is_log = df["address"].notna()          # log rows carry the emitter address
    effective_logs = (df[is_log & ~df["flag_reverted"]]
                      .groupby("hash").size()
                      .reindex(r.index[r["status"] == 1], fill_value=0))
    bad = effective_logs != r.loc[effective_logs.index, "logCount"]
    if bad.any():
        raise ValueError(f"Check 2: {int(bad.sum())} successful transactions where "
                         f"unflagged log count != receipt logCount, e.g. "
                         f"{list(effective_logs.index[bad][:5])}")


def load_if_not_found_in_state(resources_dir, file_name_snipped, state, CONFIG):
    
    base_contract = state["base_contract"]
    base = os.path.join(resources_dir, CONFIG["log_folder"], "decoding",
                        f"{file_name_snipped}_{base_contract}_{CONFIG['min_block']}_{CONFIG['max_block']}")
    # Parquet is the canonical form; pickle and CSV are read when present.
    if os.path.exists(base + ".parquet"):
        data = pd.read_parquet(base + ".parquet")
        logger.info(f"Loaded {file_name_snipped} from parquet file.")
    elif os.path.exists(base + ".pkl"):
        data = pd.read_pickle(base + ".pkl")
        logger.info(f"Loaded {file_name_snipped} from pickle file.")
    elif os.path.exists(base + ".csv"):
        data = pd.read_csv(base + ".csv")
        logger.info(f"Loaded {file_name_snipped} from CSV file.")
    else:
        raise FileNotFoundError(f"No parquet, pickle or CSV file for {file_name_snipped} at {base}.")
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
    # token_map is keyed on the era float rendering ('0.0'…'4.0'; mappings.json is
    # kept byte-identical) — coerce to numeric so the string-keyed lookup matches
    if "tokenType" in events.columns:
        events["tokenType"] = pd.to_numeric(events["tokenType"], errors="coerce")
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
    from src.trace_based_logging import storage
    from src.trace_based_logging.decoding import sanitize_for_parquet
    file_base = f"{category_name}_{base_contract}_{min_block}_{max_block}"
    parquet_path = os.path.join(resources_dir, log_folder, "transformation", file_base + ".parquet")
    storage.write_atomic_parquet(parquet_path, sanitize_for_parquet(df))
    logger.info(f"Saved {category_name} to {parquet_path}")

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
    # Revert status is the per-operation flag_reverted column computed on the
    # trace tree (flag_reverted_operations) and audited against the receipts; the
    # set-based path in initial_transformation_* applies only to inputs without
    # the column.
    txs_reverted = frozenset()
    
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
        # free each category after persisting: the five DApp frames together
        # exceed the memory of an off-the-shelf machine
        del events_dapp
        state.pop("dapp_events_decoded", None)
    else:
        logger.info("Skipping EVENTS DAPP.")
    
    # Process CALLS DAPP
    if CONFIG.get("dapp_calls", False):
        calls_dapp = transform_calls_dapp(state, resources_dir, CONFIG,
                                          mappings, creations, contracts_dapp, txs_reverted,
                                          sensitive_events=CONFIG.get("sensitive_events", False),
                                          market_info=market_info, market_type_info=market_type_info)
        save_transformed_category(calls_dapp, "dapp_calls", base_contract, min_block, max_block, resources_dir, log_folder)
        del calls_dapp
        state.pop("dapp_calls_decoded", None)
    else:
        logger.info("Skipping CALLS DAPP.")
    
    # Process DELEGATECALLS DAPP
    if CONFIG.get("dapp_delegatecalls", False):
        dcalls_dapp = transform_delegatecalls_dapp(state, resources_dir, CONFIG,
                                                    mappings, creations, contracts_dapp, txs_reverted,
                                                    sensitive_events=CONFIG.get("sensitive_events", False),
                                                    market_info=market_info, market_type_info=market_type_info)
        save_transformed_category(dcalls_dapp, "dapp_delegatecalls", base_contract, min_block, max_block, resources_dir, log_folder)
        del dcalls_dapp
        state.pop("dapp_delegatecalls_decoded", None)
    else:
        logger.info("Skipping DELEGATECALLS DAPP.")
    
    # Process ZERO VALUE CALLS DAPP
    if CONFIG.get("dapp_zero_value_calls", False):
        zcalls_dapp = transform_zero_value_calls_dapp(state, resources_dir, CONFIG,
                                                       mappings, creations, contracts_dapp, txs_reverted,
                                                       sensitive_events=CONFIG.get("sensitive_events", False),
                                                       market_info=market_info, market_type_info=market_type_info)
        save_transformed_category(zcalls_dapp, "dapp_zero_value_calls", base_contract, min_block, max_block, resources_dir, log_folder)
        del zcalls_dapp
        state.pop("dapp_zero_value_calls_decoded", None)
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

