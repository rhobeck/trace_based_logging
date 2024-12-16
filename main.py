import raw_trace_retriever.get_transactions as get_transactions
import raw_trace_retriever.get_txIndex as get_txIndex
import raw_trace_retriever.trace_transformation as trace_transformation
import raw_trace_retriever.create_relations as create_relations
import raw_trace_retriever.helpers as helpers
import trace_decoder.data_preparation as data_preparation
import pandas as pd
import time
import os 
import pickle
import json
from logging_config import setup_logging


"""
Before starting, configure the config.json file in the folder of this main.py
set_contracts_lx (list): List of contract addresses.
min_block (int): Minimum block number for transaction query.
max_block (int): Maximum block number for transaction query.
node_url (str): URL of the Ethereum node to connect to.
extract_normal_transactions (bool): Flag to determine if normal transactions should be fetched.
extract_internal_transactions (bool): Flag to determine if internal transactions should be fetched.
extract_transactions_by_events (bool): Flag to determine if transactions should be fetched based on events.
decode_events (bool): true.
decode_calls (bool): true.
decode_delegatecalls (bool): true.
decode_dapp (bool): true.
decode_non_dapp (bool):true.
"""

# Create a logger
logger = setup_logging()

# Kitties
#list_contracts_lx = ["0x06012c8cf97bead5deae237070f9587f8e7a266d", "0xb1690c08e213a35ed9bab7b318de14420fb57d8c", "0xc7af99fe5513eb6710e6d5f44f9989da40f27f26"]

#min_block = 11280000
#max_block = 11300000
#max_block = 12243999
#min_block = 4605167
#max_block = 4617765

class ExtractionState:
    def __init__(self, config):
        # Initialize state variables
        self.contracts_lx = set(map(helpers.low, config["list_contracts"]))
        self.base_contract = next(iter(self.contracts_lx))
        self.contracts_dapp = self.contracts_lx.copy()
        self.all_transactions = set()
        self.trace_tree = pd.DataFrame()
        self.non_dapp_contracts = set(map(helpers.low, config["list_predefined_non_dapp_contracts"]))


def load_config(config_path):
    try:
        with open(config_path, 'r') as file:
            config = json.load(file)
        validate_config(config)
        return config
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Error loading configuration file: {e}")
        raise
        
        
def validate_config(config):
    """
    Validate the configuration settings.
    """
    required_keys = ["port", 
                     "protocol", 
                     "host", 
                     "list_contracts", 
                     "min_block", 
                     "max_block", 
                     "extract_normal_transactions",
                     "extract_internal_transactions",
                     "extract_transactions_by_events",
                     "dapp_decode_events",
                     "dapp_decode_calls_with_ether_transfer",
                     "dapp_decode_calls_with_no_ether_transfer",
                     "dapp_decode_delegatecalls",
                     "non_dapp_decode_events",
                     "non_dapp_decode_calls_with_ether_transfer",
                     "non_dapp_decode_calls_with_no_ether_transfer",
                     "non_dapp_decode_delegatecalls",
                     "etherscan_api_key",
                     "list_predefined_non_dapp_contracts"]
                     
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required configuration key: {key}")


def build_node_url(config):
    """Construct the Ethereum node URL from the config."""
    return f"{config['protocol']}{config['host']}:{config['port']}"


def initialize_extraction_state(config):
    """Initialize the state required for transaction extraction."""
    # Only lower-case for easier string comparison
    contracts_lx = list(map(helpers.low, config["list_contracts"])) 
    non_dapp_contracts = list(map(helpers.low, config["list_predefined_non_dapp_contracts"]))

    return {
        "contracts_lx": set(contracts_lx),
        "base_contract": contracts_lx[0],
        "contracts_dapp": set(contracts_lx),
        "all_transactions": set(),
        "trace_tree": pd.DataFrame(),
        "non_dapp_contracts": set(non_dapp_contracts) 
    }


def fetch_transactions(config, contracts_lx):
    transactions = pd.DataFrame()
        
    # Get normal transactions
    if config["extract_normal_transactions"]:
        try:
            transactions_normal = get_transactions.get_transactions(contracts_lx, config["min_block"], config["max_block"], internal_flag="normal", etherscan_api_key=config["etherscan_api_key"])
            transactions = pd.concat([transactions, transactions_normal])
        except Exception as e:
            logger.error(f"Error fetching normal transactions: {e}")
    
    # Get internal transactions
    if config["extract_internal_transactions"]:
        try:
            transactions_internal = get_transactions.get_transactions(contracts_lx, config["min_block"], config["max_block"], internal_flag="internal", etherscan_api_key=config["etherscan_api_key"])
            transactions = pd.concat([transactions, transactions_internal])
        except Exception as e:
            logger.error(f"Error fetching internal transactions: {e}")

    # Get transactions by events
    if config["extract_transactions_by_events"]:
        try:
            transactions_events = get_transactions.get_transactions_by_events(build_node_url(config), contracts_lx, config["min_block"], config["max_block"])
            transactions = pd.concat([transactions, transactions_events])
        except Exception as e:
            logger.error(f"Error fetching transactions by events: {e}")
            
    # Remove duplicates from transactions, only unique transactions are needed
    transactions.drop_duplicates(subset='hash', keep="last", inplace=True)
        
    return transactions

def process_transactions(config, state):
    level = 1
    while state["contracts_lx"]:      
        logger.info("##### GETTING TRANSACTIONS #####")
        transactions = fetch_transactions(config, state["contracts_lx"])
        # Consider only transactions that have not already been looked at
        transactions = transactions[~transactions["hash"].isin(state["all_transactions"])]

        ### Breaking the loop ###
        # If no additional txs were found, the search is over
        if transactions.empty == True:
            logger.info("No additional transactions were found. The data extraction ends as planned.")
            break
        
        logger.info(f"Number of new transactions for the next iteration: {len(transactions)}")
        # Reindex, to be able to loop later
        transactions.reset_index(drop=True, inplace=True)

        # Update the set of all transactions that have been included to be able to check in the next iteration, if the trace for that transactios still has to be created, or not (if it was created already, it is in this list)
        state["all_transactions"].update(transactions["hash"].tolist())

        logger.info("##### COMPUTING TRACES #####")        
        
        # Create traces of newly added transactions
        traces = trace_transformation.tx_to_trace(transactions, build_node_url(config))
        state["trace_tree"] = pd.concat([state["trace_tree"], traces], axis=0)
        
        logger.info("SUCCESS: Transactions were retrieved and traces were recomputed and transformed.")

        logger.info("STARTING: Identifing relevant CREATE-relations.")
        state["contracts_dapp"], state["contracts_lx"] = create_relations.create_relations(state["trace_tree"], state["contracts_dapp"], state["contracts_lx"], state["non_dapp_contracts"])
            
        logger.info(f"LIST OF NEW CONTRACTS LEVEL {level} COUNT: {len(state['contracts_lx'])}")

        level = level+1
        
        logger.info(f"THIS WAS LOOP {level-1}")
        
        time.sleep(1)

    logger.info(f"Total number of extracted operations: {len(state['trace_tree'])}")
    

def insert_transaction_index(config, state):
    # retrieve transaction index (order of transactions in the trace)
    # TODO asynchronous requests
    hash_list = state["trace_tree"]["hash"].unique()
    transaction_indexes = get_txIndex.get_txIndex(hash_list, build_node_url(config))       
    state["trace_tree"]["transactionIndex"] = state["trace_tree"]["hash"].astype(str).map(transaction_indexes).fillna(state["trace_tree"]["hash"])
    
def save_trace_data(config, state, dir_path):
    # Save data as CSV and as pickle file
    path = os.path.join(dir_path, "resources", "df_trace_tree_" + state["base_contract"] + "_" + str(config["min_block"]) + "_" + str(config["max_block"]) + ".csv")
    state["trace_tree"].to_csv(path)
    path = os.path.join(dir_path, "resources", "df_trace_tree_" + state["base_contract"] + "_" + str(config["min_block"]) + "_" + str(config["max_block"]) + ".pkl")
    state["trace_tree"].to_pickle(path) 

    # free up memory
    #del state["trace_tree"]

    # Save list of contracts as TXT and as pickle file to have a list of contract belonging to the DApp available
    path = os.path.join(dir_path, "resources", "contracts_dapp_" + state["base_contract"] + "_" + str(config["min_block"]) + "_" + str(config["max_block"]) + ".txt")
    with open(path, "w") as output:
        output.write(str(state["contracts_dapp"]))
    path = os.path.join(dir_path, "resources", "contracts_dapp_" + state["base_contract"] + "_" + str(config["min_block"]) + "_" + str(config["max_block"]) + ".pkl")
    pickle.dump(state["contracts_dapp"], open(path, 'wb'))
    logger.info("DONE WITH THE DATA EXTRACTION, TRACE FILES SAVED")


def process_events(df_log, decode_flag, file_name_snipped, description, state, config, dict_abi, dir_path):
    if decode_flag:
        logger.info(f"Starting to decode EVENTS of {description} contracts")
        
        mask = df_log["address"].isin(state["contracts_dapp"])
        df_events = df_log[mask]
        
        df_events = data_preparation.decode_events(df_events, dict_abi)

        # Save the decoded events as CSV and pickle.
        path_csv = os.path.join(
            dir_path, "resources", f"{file_name_snipped}{state['base_contract']}_{config['min_block']}_{config['max_block']}.csv"
        )
        df_events.to_csv(path_csv)

        path_pkl = os.path.join(
            dir_path, "resources", f"{file_name_snipped}{state['base_contract']}_{config['min_block']}_{config['max_block']}.pkl"
        )
        with open(path_pkl, 'wb') as f:
            pickle.dump(df_events, f)

        # Free up memory
        del df_events
    else:
        logger.info(f"Skipping EVENTS of {description} contracts. Reason: 'false' flag in config file.")


def process_calls(df_log, decode_flag, file_name_snipped, calltype_list, include_zero_value_transactions, logging_string, description, state, config, dict_abi, dir_path):
    if decode_flag:
        logger.info(f"Starting to decode {logging_string} to {description} contracts")
        
        mask = df_log["to"].isin(state["contracts_dapp"])
        df_functions = df_log[mask]

        df_functions = data_preparation.decode_functions(
            df_functions, dict_abi, build_node_url(config), calltype_list, include_zero_value_transactions, logging_string
        )

        path_csv = os.path.join(
            dir_path, "resources", f"{file_name_snipped}{state['base_contract']}_{config['min_block']}_{config['max_block']}.csv"
        )
        df_functions.to_csv(path_csv)

        path_pkl = os.path.join(
            dir_path, "resources", f"{file_name_snipped}{state['base_contract']}_{config['min_block']}_{config['max_block']}.pkl"
        )
        with open(path_pkl, 'wb') as f:
            pickle.dump(df_functions, f)

        # Free up memory
        del df_functions
    else:
        logger.info(f"Skipping {logging_string} to {description} contracts. Reason: 'false' flag in config file.")

def process_delegatecalls(df_log, decode_flag, file_name_snipped, calltype_list, include_zero_value_transactions, logging_string, description, state, config, dict_abi, dir_path):
    if decode_flag:
        logger.info(f"Starting to decode DELEGATECALLs to {description} contracts")
        
        mask = df_log["to"].isin(state["contracts_dapp"])
        df_delegate = df_log[mask]

        df_delegate = data_preparation.decode_functions(
            df_delegate, dict_abi, build_node_url(config), calltype_list, include_zero_value_transactions, logging_string
        )

        path_csv = os.path.join(
            dir_path, "resources", f"{file_name_snipped}{state['base_contract']}_{config['min_block']}_{config['max_block']}.csv"
        )
        df_delegate.to_csv(path_csv)

        path_pkl = os.path.join(
            dir_path, "resources", f"{file_name_snipped}{state['base_contract']}_{config['min_block']}_{config['max_block']}.pkl"
        )
        with open(path_pkl, 'wb') as f:
            pickle.dump(df_delegate, f)

        # Free up memory
        del df_delegate
    else:
        logger.info(f"Skipping DELEGATECALLs to {description} contracts. Reason: 'false' flag in config file.")

def process_creations(df_log, dir_path, state, config):         
        logger.info("Starting to save CREATE-relations")

        mask_create = df_log["calltype"].isin(["CREATE", "CREATE2"])
        df_creations = df_log[mask_create]

        path_csv = os.path.join(
            dir_path, "resources", f"df_creations_{state['base_contract']}_{config['min_block']}_{config['max_block']}.csv"
        )
        df_creations.to_csv(path_csv)

        path_pkl = os.path.join(
            dir_path, "resources", f"df_creations_{state['base_contract']}_{config['min_block']}_{config['max_block']}.pkl"
        )
        with open(path_pkl, 'wb') as f:
            pickle.dump(df_creations, f)

        # Free up memory
        del df_creations

def main(): 
    """
    Main function to orchestrate the retrieval, processing, and saving of blockchain transaction trace data.

    This function performs several steps:
    1. Reads configuration from 'config.json' for node URL, contract list, block range, and extraction flags.
    2. Retrieves normal, internal transactions, and transactions by events based on the configuration flags.
    3. Processes and transforms the transaction data, including trace transformation and relation creation.
    4. Decodes the data for events, function calls, and delegate calls.
    5. Saves the processed data in various formats (CSV, pickle) for further analysis or processing, e.g., creating meaningful event logs.

    The function utilizes several modules to accomplish these tasks, including:
    - `get_transactions` for fetching transaction data.
    - `trace_transformation` for transforming transaction traces.
    - `create_relations` for identifying CREATE-relationships beween smart contracts within the data.
    - `data_preparation` for preprocessing and decoding transaction data.

    The process is logged using the configured logger, with errors and information at code execution.

    Raises:
        ValueError: If no transaction data is fetched, indicating a potential issue in data extraction or empty dataset.

    Note:
        Before executing, ensure the 'config.json' file in the script's directory is properly configured with 
        the necessary parameters such as node URL, contract addresses, block range, and extraction flags.
        Please also note that this is a research prototype. The tool is not fully documented, not fully tested and not all errors are caught. Some implementations are not ideal, e.g., inserting the position in a trace by manipulating JSON-attributes or building dataframes iteratively.
    """
    dir_path = os.path.dirname(os.path.realpath(__file__))
    try:
        config = load_config(os.path.join(dir_path, 'config.json'))
        state = initialize_extraction_state(config)
        helpers.check_socket(config["host"], config["port"])
        process_transactions(config, state)
        insert_transaction_index(config, state)
        save_trace_data(config, state, dir_path)
        
        
    except Exception as e:
        logger.error(f"Error in the extraction process: {e}")



    ################ DECODING THE LOG ################
    """
    At this point a transaction traces were extracted. 
    However, the data in the traces is encoded. 
    Hence, the following script heplps to decode the data.
    
    The decoding can be done for different types in the data.
    There is a general distinction between: 
        - Data / traces that belong to the DApp and
        - Data / traces that do NOT belong to the DApp
    Data entries that can be decoded are: 
        - Events, 
        - CALLs, and 
        - DELEGATECALLs
    The CALLs and DELEGATECALLs can be associated:
        - With Ether / value transfer
        - Without Ether / value transfer
    Although dependent on the use case, most likely whats relevant is: 
        - Data traces belonging to the DApp 
        - with events, CALLs, and DELEGATECALLs
        - with Ether transfer 
    """
    try:
        logger.info("STARTING TO DECODE DATA")

        df_log = data_preparation.base_transformation(state["trace_tree"], state["contracts_dapp"])
        #free-up memory
        del state["trace_tree"]

        addresses = data_preparation.address_selection(df_log)
        
        # Decoding requires extra information, e.g., event specifications, function specifications
        # That data is available in the ABIs of the contracts
        # So next, the ABIs are retrieved from Etherscan (if possible) 
        logger.info("Starting to retrieve ABIs.")
        
        if (config["non_dapp_decode_events"] == False) and (config["non_dapp_decode_calls_with_ether_transfer"] == False) and (config["non_dapp_decode_calls_with_no_ether_transfer"] == False) and (config["non_dapp_decode_delegatecalls"] == False):
            logger.info("Only retrieving DApp CA ABIs. Decoding of non-DApp CAs disabled in config-file.")
            addresses = list(set(addresses) & set(state["contracts_dapp"]))
        
        dict_abi = data_preparation.create_abi_dict(addresses, config["etherscan_api_key"])

        path = os.path.join(dir_path, "resources", "dict_abi_" + state["base_contract"] + "_" + str(config["min_block"]) + "_" + str(config["max_block"]) + ".pkl")
        pickle.dump(dict_abi, open(path, 'wb'))

        # Process DApp Events
        process_events(df_log, config["dapp_decode_events"], "df_events_dapp_", "DApp", state, config, dict_abi, dir_path)

        # Process DApp CALLs with Ether Transfer
        process_calls(df_log, config["dapp_decode_calls_with_ether_transfer"], "df_call_dapp_with_ether_transfer_", ["CALL"], False, "CALLs with Ether transfer", "DApp", state, config, dict_abi, dir_path)

        # Process DApp CALLs with No Ether Transfer
        process_calls(df_log, config["dapp_decode_calls_with_no_ether_transfer"], "df_call_dapp_with_no_ether_transfer_", ["CALL"], True, "CALLs with no Ether transfer", "DApp", state, config, dict_abi, dir_path)

        # Process DApp DELEGATECALLs
        process_delegatecalls(df_log, config["dapp_decode_delegatecalls"], "df_delegatecall_dapp_", ["DELEGATECALL"], False, "DELEGATECALLs", "DApp", state, config, dict_abi, dir_path)

        # Process Non-DApp Events
        process_events(df_log, config["non_dapp_decode_events"], "df_events_non_dapp_", "NON-DApp", state, config, dict_abi, dir_path)

        # Process Non-DApp CALLs with Ether Transfer
        process_calls(df_log, config["non_dapp_decode_calls_with_ether_transfer"], "df_call_with_ether_transfer_non_dapp_", ["CALL"], False, "CALLs with Ether transfer", "NON-DApp", state, config, dict_abi, dir_path)

        # Process Non-DApp CALLs with No Ether Transfer
        process_calls(df_log, config["non_dapp_decode_calls_with_no_ether_transfer"], "df_call_with_no_ether_transfer_non_dapp_", ["CALL"], True, "CALLs with no Ether transfer", "NON-DApp", state, config, dict_abi, dir_path)

        # Process Non-DApp DELEGATECALLs
        process_delegatecalls(df_log, config["non_dapp_decode_delegatecalls"], "df_delegatecall_non_dapp_", ["DELEGATECALL"], False, "DELEGATECALLs", "NON-DApp", state, config, dict_abi, dir_path)

        process_creations(df_log, dir_path, state, config)
        
    except Exception as e:
        logger.error(f"Error in the decoding process: {e}")
    """
    For handling log construction, there is a module implemented individually and currently stand-alone.
    Please note that the decoded trace can be used as an event log per se.
    However, the log can be improved by case-specific editing, e.g., defining more meaningful event names, creating objects in the data, etc.
    Since the editing is case specific, the module log_construction is an example for log construction of an Augur log.
    """
    print("DONE")
    
    pass

if __name__ == "__main__":
    main()



