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
    process_events(df_log, config["dapp_decode_events"], "df_events_dapp_", "DApp", state, config, dict_abi, dir_path)
    process_calls(df_log, config["dapp_decode_calls_with_ether_transfer"], "df_call_dapp_with_ether_transfer_", ["CALL"], False, "CALLs with Ether transfer", "DApp", state, config, dict_abi, dir_path)
    process_calls(df_log, config["dapp_decode_calls_with_no_ether_transfer"], "df_call_dapp_with_no_ether_transfer_", ["CALL"], True, "CALLs with no Ether transfer", "DApp", state, config, dict_abi, dir_path)
    process_delegatecalls(df_log, config["dapp_decode_delegatecalls"], "df_delegatecall_dapp_", ["DELEGATECALL"], False, "DELEGATECALLs", "DApp", state, config, dict_abi, dir_path)
    process_events(df_log, config["non_dapp_decode_events"], "df_events_non_dapp_", "NON-Dapp", state, config, dict_abi, dir_path)
    process_calls(df_log, config["non_dapp_decode_calls_with_ether_transfer"], "df_call_with_ether_transfer_non_dapp_", ["CALL"], False, "CALLs with Ether transfer", "NON-Dapp", state, config, dict_abi, dir_path)
    process_calls(df_log, config["non_dapp_decode_calls_with_no_ether_transfer"], "df_call_with_no_ether_transfer_non_dapp_", ["CALL"], True, "CALLs with no Ether transfer", "NON-Dapp", state, config, dict_abi, dir_path)
    process_delegatecalls(df_log, config["non_dapp_decode_delegatecalls"], "df_delegatecall_non_dapp_", ["DELEGATECALL"], False, "DELEGATECALLs", "NON-Dapp", state, config, dict_abi, dir_path)
    process_creations(df_log, dir_path, state, config)
    logger.info("Decoding process complete.")

def process_events(df_log, decode_flag, file_name_snipped, description, state, config, dict_abi, dir_path):
    from src.trace_based_logging.trace_decoder import data_preparation
    if decode_flag:
        logger.info(f"Decoding EVENTS for {description} contracts")
        mask = df_log["address"].isin(state["contracts_dapp"])
        df_events = df_log[mask]
        df_events = data_preparation.decode_events(df_events, dict_abi)
        path_csv = os.path.join(dir_path, "resources", f"{file_name_snipped}{state['base_contract']}_{config['min_block']}_{config['max_block']}.csv")
        df_events.to_csv(path_csv)
        path_pkl = os.path.join(dir_path, "resources", f"{file_name_snipped}{state['base_contract']}_{config['min_block']}_{config['max_block']}.pkl")
        with open(path_pkl, 'wb') as f:
            pickle.dump(df_events, f)
        del df_events
    else:
        logger.info(f"Skipping EVENTS for {description} (flag false).")

def process_calls(df_log, decode_flag, file_name_snipped, calltype_list, include_zero_value_transactions, logging_string, description, state, config, dict_abi, dir_path):
    from src.trace_based_logging.trace_decoder import data_preparation
    if decode_flag:
        logger.info(f"Decoding {logging_string} for {description} contracts")
        mask = df_log["to"].isin(state["contracts_dapp"])
        df_functions = df_log[mask]
        df_functions = data_preparation.decode_functions(
            df_functions, dict_abi, build_node_url(config), calltype_list, include_zero_value_transactions, logging_string
        )
        path_csv = os.path.join(dir_path, "resources", f"{file_name_snipped}{state['base_contract']}_{config['min_block']}_{config['max_block']}.csv")
        df_functions.to_csv(path_csv)
        path_pkl = os.path.join(dir_path, "resources", f"{file_name_snipped}{state['base_contract']}_{config['min_block']}_{config['max_block']}.pkl")
        with open(path_pkl, 'wb') as f:
            pickle.dump(df_functions, f)
        del df_functions
    else:
        logger.info(f"Skipping {logging_string} for {description} (flag false).")

def process_delegatecalls(df_log, decode_flag, file_name_snipped, calltype_list, include_zero_value_transactions, logging_string, description, state, config, dict_abi, dir_path):
    from src.trace_based_logging.trace_decoder import data_preparation
    if decode_flag:
        logger.info(f"Decoding DELEGATECALLs for {description} contracts")
        mask = df_log["to"].isin(state["contracts_dapp"])
        df_delegate = df_log[mask]
        df_delegate = data_preparation.decode_functions(
            df_delegate, dict_abi, build_node_url(config), calltype_list, include_zero_value_transactions, logging_string
        )
        path_csv = os.path.join(dir_path, "resources", f"{file_name_snipped}{state['base_contract']}_{config['min_block']}_{config['max_block']}.csv")
        df_delegate.to_csv(path_csv)
        path_pkl = os.path.join(dir_path, "resources", f"{file_name_snipped}{state['base_contract']}_{config['min_block']}_{config['max_block']}.pkl")
        with open(path_pkl, 'wb') as f:
            pickle.dump(df_delegate, f)
        del df_delegate
    else:
        logger.info(f"Skipping DELEGATECALLs for {description} (flag false).")

def process_creations(df_log, dir_path, state, config):
    logger.info("Saving CREATE-relations")
    mask_create = df_log["calltype"].isin(["CREATE", "CREATE2"])
    df_creations = df_log[mask_create]
    path_csv = os.path.join(dir_path, "resources", f"df_creations_{state['base_contract']}_{config['min_block']}_{config['max_block']}.csv")
    df_creations.to_csv(path_csv)
    path_pkl = os.path.join(dir_path, "resources", f"df_creations_{state['base_contract']}_{config['min_block']}_{config['max_block']}.pkl")
    with open(path_pkl, 'wb') as f:
        pickle.dump(df_creations, f)
    del df_creations

def build_node_url(config):
    return f"{config['protocol']}{config['host']}:{config['port']}"
