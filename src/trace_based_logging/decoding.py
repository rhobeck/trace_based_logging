import os
import pickle
import time
from src.trace_based_logging.logging_config import setup_logging
from src.trace_based_logging.config import build_node_url

logger = setup_logging()


def save_decoded_data(state, config, file_name_snippet, dir_path):
        path_csv = os.path.join(dir_path, "resources", config["log_folder"], "decoding", f"{file_name_snippet}_{state['base_contract']}_{config['min_block']}_{config['max_block']}.csv")
        state[file_name_snippet].to_csv(path_csv)
        path_pkl = os.path.join(dir_path, "resources", config["log_folder"], "decoding", f"{file_name_snippet}_{state['base_contract']}_{config['min_block']}_{config['max_block']}.pkl")
        with open(path_pkl, 'wb') as f:
            pickle.dump(state[file_name_snippet], f)
        logger.info(f"Saved to {path_csv} and {path_pkl}")

def decode_all(df_log, state, config, dict_abi, build_node_url_func):
    """
    Orchestrates decoding: transforms raw trace data, retrieves ABIs,
    and then decodes events, function calls, delegatecalls, and creations.
    """
    logger.info("STARTING CORE DECODING PROCESS")
    # Transform raw trace data into a log DataFrame

    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

    # Process decoding steps:
    state = process_events(df_log, config["dapp_events"], "dapp_events_decoded", "DApp", state, config, dict_abi, dir_path)
    state = process_calls(df_log, config["dapp_calls"], "dapp_calls_decoded", ["CALL"], False, "CALLs with Ether transfer", "DApp", state, config, dict_abi, dir_path)
    state = process_calls(df_log, config["dapp_zero_value_calls"], "dapp_zero_value_calls_decoded", ["CALL"], True, "CALLs with no Ether transfer", "DApp", state, config, dict_abi, dir_path)
    state = process_delegatecalls(df_log, config["dapp_delegatecalls"], "dapp_delegatecalls_decoded", ["DELEGATECALL"], True, "DELEGATECALLs", "DApp", state, config, dict_abi, dir_path)
    state = process_events(df_log, config["non_dapp_events"], "non_dapp_events_decoded", "NON-DApp", state, config, dict_abi, dir_path)
    state = process_calls(df_log, config["non_dapp_calls"], "non_dapp_calls_decoded", ["CALL"], False, "CALLs with Ether transfer", "NON-DApp", state, config, dict_abi, dir_path)
    state = process_calls(df_log, config["non_dapp_zero_value_calls"], "non_dapp_zero_value_calls_decoded", ["CALL"], True, "CALLs with no Ether transfer", "NON-DApp", state, config, dict_abi, dir_path)
    state = process_delegatecalls(df_log, config["non_dapp_delegatecalls"], "non_dapp_delegatecalls_decoded", ["DELEGATECALL"], True, "DELEGATECALLs", "NON-DApp", state, config, dict_abi, dir_path)
    state = process_creations(df_log, dir_path, state, config, "creations")
    logger.info("Decoding process complete.")
    return state

def process_events(df_log, decode_flag, file_name_snippet, description, state, config, dict_abi, dir_path):
    from src.trace_based_logging.trace_decoder import data_preparation
    if decode_flag:
        logger.info(f"Decoding EVENTS for {description} contracts")
        if description == "DApp":
            mask = df_log["address"].isin(state["contracts_dapp"])
        if description == "NON-DApp":
            mask = ~df_log["address"].isin(state["contracts_dapp"])
        df_events = df_log[mask]
        df_events = data_preparation.decode_events(df_events, dict_abi)
        state[file_name_snippet] = df_events
        save_decoded_data(state, config, file_name_snippet, dir_path)
        del df_events
    else:
        logger.info(f"Skipping EVENTS for {description} (flag false).")
    return state

def process_calls(df_log, decode_flag, file_name_snippet, calltype_list, include_zero_value_transactions, logging_string, description, state, config, dict_abi, dir_path):
    from src.trace_based_logging.trace_decoder import data_preparation
    if decode_flag:
        logger.info(f"Decoding {logging_string} for {description} contracts")
        if description == "DApp":
            mask = df_log["to"].isin(state["contracts_dapp"])
        if description == "NON-DApp":
            mask = ~df_log["to"].isin(state["contracts_dapp"])
        df_functions = df_log[mask]
        df_functions = data_preparation.decode_functions(
            df_functions, dict_abi, build_node_url(config), calltype_list, include_zero_value_transactions, logging_string
        )
        state[file_name_snippet] = df_functions
        save_decoded_data(state, config, file_name_snippet, dir_path)    
        del df_functions
    else:
        logger.info(f"Skipping {logging_string} for {description} (flag false).")
    return state

def process_delegatecalls(df_log, decode_flag, file_name_snippet, calltype_list, include_zero_value_transactions, logging_string, description, state, config, dict_abi, dir_path):
    from src.trace_based_logging.trace_decoder import data_preparation
    if decode_flag:
        logger.info(f"Decoding DELEGATECALLs for {description} contracts")
        if description == "DApp":
            mask = df_log["to"].isin(state["contracts_dapp"])
        if description == "NON-DApp":
            mask = ~df_log["to"].isin(state["contracts_dapp"])
        df_delegate = df_log[mask]
        df_delegate = data_preparation.decode_functions(
            df_delegate, dict_abi, build_node_url(config), calltype_list, include_zero_value_transactions, logging_string
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

def build_node_url(config):
    return f"{config['protocol']}{config['host']}:{config['port']}"
