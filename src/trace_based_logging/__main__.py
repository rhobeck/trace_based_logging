import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import pickle
from src.trace_based_logging.logging_config import setup_logging

logger = setup_logging()

# Import our new modules from the package
from src.trace_based_logging.config import load_config, build_node_url, initialize_extraction_state
from src.trace_based_logging.extraction import process_transactions, insert_transaction_index
from src.trace_based_logging.saving import save_trace_data
from src.trace_based_logging.decoding import decode_all

def main():
    # Compute directory path; here __main__.py is inside trace_based_logging/
    #dir_path = os.path.dirname(os.path.realpath(__file__))
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    config_path = os.path.join(dir_path, 'config.json')
    
    try:
        config = load_config(config_path)
        # Use the raw_trace_retriever's utils for string lowering and such.
        from src.trace_based_logging.raw_trace_retriever import trace_retriever_utils
        state = initialize_extraction_state(config, trace_retriever_utils)
        trace_retriever_utils.check_socket(config["host"], config["port"])
        
        process_transactions(config, state)
        insert_transaction_index(config, state, build_node_url)
        dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        save_trace_data(config, state, dir_path)
    except Exception as e:
        logger.error(f"Error in extraction phase: {e}")
    
    try:
        logger.info("Starting decoding phase")
        from src.trace_based_logging.trace_decoder import data_preparation
        # Prepare the log DataFrame from the extracted trace data
        df_log = data_preparation.base_transformation(state["trace_tree"], state["contracts_dapp"])
        del state["trace_tree"]
        dict_abi = data_preparation.create_abi_dict(data_preparation.address_selection(df_log), config["etherscan_api_key"])
        dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        abi_path = os.path.join(dir_path, "resources", f"dict_abi_{state['base_contract']}_{config['min_block']}_{config['max_block']}.pkl")
        pickle.dump(dict_abi, open(abi_path, 'wb'))
        decode_all(df_log, state, config, dict_abi, build_node_url)
    except Exception as e:
        logger.error(f"Error in decoding phase: {e}")

    try:
        logger.info("Starting transformation phase")
        from src.trace_based_logging.log_construction import transformation_augur
        dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        RESOURCES_DIR = os.path.join(dir_path, "resources")
    
        LOG_FOLDER = config["log_folder"]

        transformation_augur.transform_augur_data(RESOURCES_DIR, LOG_FOLDER, config, state["base_contract"], build_node_url)
    except Exception as e:
        logger.error(f"Error in transformation phase: {e}")
                
    print("DONE")

if __name__ == '__main__':
    main()
