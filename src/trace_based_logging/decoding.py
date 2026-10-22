"""The decoding stage: splits the trace operations into the categories of Table 2 of
the paper (log entries, calls with and without Ether, delegated calls, creations;
DApp and non-DApp), decodes each with the contract ABIs, and writes the decoded
tables to the run folder.

Functions: sanitize_for_parquet, save_decoded_data, decode_all, process_events,
process_calls, process_delegatecalls, process_creations.
"""
import os

import pandas as pd

from src.trace_based_logging.logging_config import setup_logging
from src.trace_based_logging.config import build_node_url
from src.trace_based_logging import storage
from src.trace_based_logging.trace_decoder import data_preparation

logger = setup_logging()


def sanitize_for_parquet(df):
    """Return a parquet-safe copy: object columns containing non-scalar or oversized
    values (decoded tuples/lists, bytes, uint256 integers) are stringified. The
    in-memory frames passed between stages remain untouched; this applies to the
    persisted form only.
    """
    out = df.copy()
    int64_max = 2 ** 63 - 1

    def is_scalar_notna(v):
        # pd.notna returns an array for list-likes; treat those as "present"
        res = pd.notna(v)
        return bool(res) if isinstance(res, (bool,)) or not hasattr(res, "__len__") else True

    def needs_str(v):
        return isinstance(v, (bytes, bytearray, list, tuple, dict, set)) or \
               (isinstance(v, int) and abs(v) > int64_max)

    for col in out.columns:
        if out[col].dtype == object:
            cells = out[col].map(lambda v: v if is_scalar_notna(v) else None)
            has_needs_str = cells.map(lambda v: v is not None and needs_str(v)).any()
            # mixed str/numeric object columns (e.g. a concat of category tables
            # whose dtypes diverged) are equally unwritable — stringify those too
            kinds = {type(v) for v in cells if v is not None}
            mixed = str in kinds and len(kinds) > 1
            if has_needs_str or mixed:
                out[col] = out[col].map(
                    lambda v: ("0x" + v.hex()) if isinstance(v, (bytes, bytearray))
                    else (str(v) if is_scalar_notna(v) else v))
    return out


def save_decoded_data(state, config, file_name_snippet, dir_path):
    path = os.path.join(dir_path, "resources", config["log_folder"], "decoding",
                        f"{file_name_snippet}_{state['base_contract']}_"
                        f"{config['min_block']}_{config['max_block']}.parquet")
    storage.write_atomic_parquet(path, sanitize_for_parquet(state[file_name_snippet]))
    logger.info(f"Saved to {path}")


def decode_all(df_log, state, config, dict_abi, build_node_url_func):
    """
    Orchestrates decoding: decodes events, function calls, delegatecalls, and
    creations into the eight config-toggled output categories (the paper's output
    matrix). Contract objects are built once and shared across all passes.
    """
    logger.info("STARTING CORE DECODING PROCESS")

    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    node_url = build_node_url_func(config)
    contract_objects, failed = data_preparation.build_contract_objects(dict_abi, node_url)
    if failed:
        logger.warning(f"{len(failed)} ABI(s) could not be turned into contract objects.")

    state = process_events(df_log, config["dapp_events"], "dapp_events_decoded", "DApp", state, config, dict_abi, dir_path)
    state = process_calls(df_log, config["dapp_calls"], "dapp_calls_decoded", ["CALL"], False, "CALLs with Ether transfer", "DApp", state, config, dict_abi, dir_path, contract_objects)
    state = process_calls(df_log, config["dapp_zero_value_calls"], "dapp_zero_value_calls_decoded", ["CALL"], True, "CALLs with no Ether transfer", "DApp", state, config, dict_abi, dir_path, contract_objects)
    state = process_delegatecalls(df_log, config["dapp_delegatecalls"], "dapp_delegatecalls_decoded", ["DELEGATECALL"], None, "DELEGATECALLs", "DApp", state, config, dict_abi, dir_path, contract_objects)
    state = process_events(df_log, config["non_dapp_events"], "non_dapp_events_decoded", "NON-DApp", state, config, dict_abi, dir_path)
    state = process_calls(df_log, config["non_dapp_calls"], "non_dapp_calls_decoded", ["CALL"], False, "CALLs with Ether transfer", "NON-DApp", state, config, dict_abi, dir_path, contract_objects)
    state = process_calls(df_log, config["non_dapp_zero_value_calls"], "non_dapp_zero_value_calls_decoded", ["CALL"], True, "CALLs with no Ether transfer", "NON-DApp", state, config, dict_abi, dir_path, contract_objects)
    state = process_delegatecalls(df_log, config["non_dapp_delegatecalls"], "non_dapp_delegatecalls_decoded", ["DELEGATECALL"], None, "DELEGATECALLs", "NON-DApp", state, config, dict_abi, dir_path, contract_objects)
    state = process_creations(df_log, dir_path, state, config, "creations")
    logger.info("Decoding process complete.")
    return state


def process_events(df_log, decode_flag, file_name_snippet, description, state, config, dict_abi, dir_path):
    if decode_flag:
        logger.info(f"Decoding EVENTS for {description} contracts")
        if description == "DApp":
            mask = df_log["address"].isin(state["contracts_dapp"])
        else:
            mask = ~df_log["address"].isin(state["contracts_dapp"])
        df_events = df_log[mask]
        df_events = data_preparation.decode_events(df_events, dict_abi)
        state[file_name_snippet] = df_events
        save_decoded_data(state, config, file_name_snippet, dir_path)
        del df_events
    else:
        logger.info(f"Skipping EVENTS for {description} (flag false).")
    return state


def process_calls(df_log, decode_flag, file_name_snippet, calltype_list, include_zero_value_transactions, logging_string, description, state, config, dict_abi, dir_path, contract_objects=None):
    if decode_flag:
        logger.info(f"Decoding {logging_string} for {description} contracts")
        if description == "DApp":
            mask = df_log["to"].isin(state["contracts_dapp"])
        else:
            mask = ~df_log["to"].isin(state["contracts_dapp"])
        df_functions = df_log[mask]
        df_functions = data_preparation.decode_functions(
            df_functions, dict_abi, build_node_url(config), calltype_list,
            include_zero_value_transactions, logging_string, contract_objects=contract_objects
        )
        state[file_name_snippet] = df_functions
        save_decoded_data(state, config, file_name_snippet, dir_path)
        del df_functions
    else:
        logger.info(f"Skipping {logging_string} for {description} (flag false).")
    return state


def process_delegatecalls(df_log, decode_flag, file_name_snippet, calltype_list, include_zero_value_transactions, logging_string, description, state, config, dict_abi, dir_path, contract_objects=None):
    if decode_flag:
        logger.info(f"Decoding DELEGATECALLs for {description} contracts")
        if description == "DApp":
            mask = df_log["to"].isin(state["contracts_dapp"])
        else:
            mask = ~df_log["to"].isin(state["contracts_dapp"])
        df_delegate = df_log[mask]
        df_delegate = data_preparation.decode_functions(
            df_delegate, dict_abi, build_node_url(config), calltype_list,
            include_zero_value_transactions, logging_string, contract_objects=contract_objects
        )
        state[file_name_snippet] = df_delegate
        save_decoded_data(state, config, file_name_snippet, dir_path)
        del df_delegate
    else:
        logger.info(f"Skipping DELEGATECALLs for {description} (flag false).")
    return state


def process_creations(df_log, dir_path, state, config, file_name_snippet):
    logger.info("Saving CREATE-relations")
    mask_create = df_log["calltype"].isin(["CREATE", "CREATE2"])
    df_creations = df_log[mask_create]
    state["creations"] = df_creations
    save_decoded_data(state, config, file_name_snippet, dir_path)
    del df_creations
    return state
