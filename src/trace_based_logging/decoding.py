import os
import pickle
import time
from src.trace_based_logging.logging_config import setup_logging
from src.trace_based_logging.config import build_node_url

logger = setup_logging()

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
    state = process_creations(df_log, dir_path, state, config)
    logger.info("Decoding process complete.")
    return state

def process_events(df_log, decode_flag, file_name_snipped, description, state, config, dict_abi, dir_path):
    from src.trace_based_logging.trace_decoder import data_preparation
    if decode_flag:
        logger.info(f"Decoding EVENTS for {description} contracts")
        if description == "DApp":
            mask = df_log["address"].isin(state["contracts_dapp"])
        if description == "NON-DApp":
            mask = ~df_log["address"].isin(state["contracts_dapp"])
        df_events = df_log[mask]
        df_events = data_preparation.decode_events(df_events, dict_abi)
        state[file_name_snipped] = df_events
        path_csv = os.path.join(dir_path, "resources", f"{file_name_snipped}_{state['base_contract']}_{config['min_block']}_{config['max_block']}.csv")
        df_events.to_csv(path_csv)
        path_pkl = os.path.join(dir_path, "resources", f"{file_name_snipped}_{state['base_contract']}_{config['min_block']}_{config['max_block']}.pkl")
        with open(path_pkl, 'wb') as f:
            pickle.dump(df_events, f)
        logger.info(f"Saved to {path_csv} and {path_pkl}")    
        del df_events
        return state
    else:
        logger.info(f"Skipping EVENTS for {description} (flag false).")

def process_calls(df_log, decode_flag, file_name_snipped, calltype_list, include_zero_value_transactions, logging_string, description, state, config, dict_abi, dir_path):
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
        state[file_name_snipped] = df_functions
        path_csv = os.path.join(dir_path, "resources", f"{file_name_snipped}_{state['base_contract']}_{config['min_block']}_{config['max_block']}.csv")
        df_functions.to_csv(path_csv)
        path_pkl = os.path.join(dir_path, "resources", f"{file_name_snipped}_{state['base_contract']}_{config['min_block']}_{config['max_block']}.pkl")
        with open(path_pkl, 'wb') as f:
            pickle.dump(df_functions, f)
        logger.info(f"Saved to {path_csv} and {path_pkl}")    
        del df_functions
        return state
    else:
        logger.info(f"Skipping {logging_string} for {description} (flag false).")

def process_delegatecalls(df_log, decode_flag, file_name_snipped, calltype_list, include_zero_value_transactions, logging_string, description, state, config, dict_abi, dir_path):
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
        state[file_name_snipped] = df_delegate
        path_csv = os.path.join(dir_path, "resources", f"{file_name_snipped}_{state['base_contract']}_{config['min_block']}_{config['max_block']}.csv")
        df_delegate.to_csv(path_csv)
        path_pkl = os.path.join(dir_path, "resources", f"{file_name_snipped}_{state['base_contract']}_{config['min_block']}_{config['max_block']}.pkl")
        with open(path_pkl, 'wb') as f:
            pickle.dump(df_delegate, f)
        logger.info(f"Saved to {path_csv} and {path_pkl}")    
        del df_delegate
        return state
    else:
        logger.info(f"Skipping DELEGATECALLs for {description} (flag false).")

def process_creations(df_log, dir_path, state, config):
    logger.info("Saving CREATE-relations")
    mask_create = df_log["calltype"].isin(["CREATE", "CREATE2"])
    df_creations = df_log[mask_create]
    state["creations"] = df_creations
    path_csv = os.path.join(dir_path, "resources", f"creations_{state['base_contract']}_{config['min_block']}_{config['max_block']}.csv")
    df_creations.to_csv(path_csv)
    path_pkl = os.path.join(dir_path, "resources", f"creations_{state['base_contract']}_{config['min_block']}_{config['max_block']}.pkl")
    with open(path_pkl, 'wb') as f:
        pickle.dump(df_creations, f)
    logger.info(f"Saved to {path_csv} and {path_pkl}")
    del df_creations
    return state

def build_node_url(config):
    return f"{config['protocol']}{config['host']}:{config['port']}"
