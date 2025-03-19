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
    
    try:
        logger.info("STARTING SET-UP PHASE")
        config = load_config(config_path)
        from src.trace_based_logging.raw_trace_retriever import trace_retriever_utils
        state = initialize_extraction_state(config, trace_retriever_utils)
        trace_retriever_utils.check_socket(config["host"], config["port"])
        folder_set_up(dir_path, config)
    except Exception as e:
        logger.error(f"Error in set-up phase: {e}")
    
    if config["extraction"]:
        try:
            logger.info("STARTING EXTRACTION PHASE")
            process_transactions(config, state)
            insert_transaction_index(config, state, build_node_url)
            save_trace_data(config, state, dir_path)
        except Exception as e:
            logger.error(f"Error in extraction phase: {e}")
    else: 
        logger.info("Skipping extraction phase.")
        
    if config["decoding"]:
        try:
            logger.info("STARTING DECODING PHASE")
            from src.trace_based_logging.trace_decoder import data_preparation
            
            if not config["extraction"]:
                try: 
                    state["trace_tree"] = data_preparation.load_data(config, state, "df_trace_tree")
                except Exception as e:
                    logger.error(f"Error loading df_log: {e}")
                    
                try: 
                    state["contracts_dapp"] = data_preparation.load_data(config, state, "contracts_dapp")
                except Exception as e:
                    logger.error(f"Error loading contracts: {e}")
            
            df_log = data_preparation.base_transformation(state["trace_tree"], state["contracts_dapp"])
            del state["trace_tree"]
            dict_abi = data_preparation.create_abi_dict(data_preparation.address_selection(df_log), config["etherscan_api_key"])
            dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
            abi_path = os.path.join(dir_path, "resources", config["log_folder"], "decoding", f"dict_abi_{state['base_contract']}_{config['min_block']}_{config['max_block']}.pkl")
            pickle.dump(dict_abi, open(abi_path, 'wb'))
            logger.info(f"Saved ABI dictionary at: {abi_path}")
            decode_all(df_log, state, config, dict_abi, build_node_url)
        except Exception as e:
            logger.error(f"Error in decoding phase: {e}")
    else: 
        logger.info("Skipping decoding phase.")
    
    if config["transformation"]:
        try:
            logger.info("STARTING TRANSFORMATION PHASE")
            from src.trace_based_logging.log_construction import transformation_augur
            from src.trace_based_logging.log_construction import log_construction_augur
            RESOURCES_DIR = os.path.join(dir_path, "resources")
            LOG_FOLDER = config["log_folder"]
            transformation_augur.transform_augur_data(RESOURCES_DIR, LOG_FOLDER, state, config)
            config["base_contract"] = state["base_contract"] # TODO: not a clean solution
            del state
            log_construction_augur.build_log(RESOURCES_DIR, LOG_FOLDER, config)
        except Exception as e:
            logger.error(f"Error in transformation phase: {e}")
    else: 
        logger.info("Skipping transformation phase.")            
    print("DONE")

if __name__ == '__main__':
    main()
