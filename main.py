import raw_trace_retriever.get_transactions as get_transactions
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

    with open('config.json', 'r') as file:
        config = json.load(file)
    
    port = config["port"]
    protocol = config["protocol"]
    host = config["host"]
    node_url = protocol + host + ":" + str(port)
    list_contracts_lx = config["list_contracts"]
    # for Augur, important creations occur around block 5926229
    min_block = config["min_block"]
    max_block = config["max_block"]
    # max_block = 11229577

    extract_normal_transactions = config["extract_normal_transactions"]
    extract_internal_transactions = config["extract_internal_transactions"]
    extract_transactions_by_events = config["extract_transactions_by_events"]
    
    dapp_decode_events = config["dapp_decode_events"]
    dapp_decode_calls_with_ether_transfer = config["dapp_decode_calls_with_ether_transfer"]
    dapp_decode_calls_with_no_ether_transfer = config["dapp_decode_calls_with_no_ether_transfer"]
    dapp_decode_delegatecalls = config["dapp_decode_delegatecalls"]
    non_dapp_decode_events = config["non_dapp_decode_events"]
    non_dapp_decode_calls_with_ether_transfer = config["non_dapp_decode_calls_with_ether_transfer"]
    non_dapp_decode_calls_with_no_ether_transfer = config["non_dapp_decode_calls_with_no_ether_transfer"]
    non_dapp_decode_delegatecalls = config["non_dapp_decode_delegatecalls"]
    
    etherscan_api_key = config["etherscan_api_key"]
    
    list_predefined_non_dapp_contracts = config["list_predefined_non_dapp_contracts"]
    
    # Kitties
    #list_contracts_lx = ["0x06012c8cf97bead5deae237070f9587f8e7a266d", "0xb1690c08e213a35ed9bab7b318de14420fb57d8c", "0xc7af99fe5513eb6710e6d5f44f9989da40f27f26"]

    #min_block = 11280000
    #max_block = 11300000
    #max_block = 12243999
    #min_block = 4605167
    #max_block = 4617765

    # Only lower-case for easier string comparison
    list_contracts_lx = list(map(helpers.low, list_contracts_lx))
    base_contract = list_contracts_lx[0]
    # Only unique contracts
    set_contracts_lx = set(list_contracts_lx)
    
    # Only lower-case for easier string comparison 
    list_predefined_non_dapp_contracts = list(map(helpers.low, list_predefined_non_dapp_contracts))
    # Only unique contracts
    set_predefined_non_dapp_contracts = set(list_predefined_non_dapp_contracts)

    level = 1

    # Set of all transactions
    list_txs_all = set()
    # dataframe to collect transactions traces
    df_trace_tree = pd.DataFrame()
    # Set of contracts belonging to the DApp
    contracts_dapp = set(set_contracts_lx)

    helpers.check_socket(host, port)

    while len(set_contracts_lx) != 0:
        """
        Loop to fetch and process transactions and transaction traces for each contract address in set_contracts_lx.
        Within every iteration of the loop, contracts belonging to the DApp are successively identified.

        Think about the deployment structure of smart contracts as trees:
            - There is a deployer as a root externally owned account
            - That root has deployed contracts
            - The deployed contracts might have deployed other contracts (e.g., using the factory pattern)
        The idea: Take one or more accounts from this tree / forest structure and rediscover the deployment tree
        Method: 
            - Find transactions to, from, and through the accounts we know within a blockrange 
            - Re-compute the traces of these transactions
            - Find CREATE-relations between accounts
            - If the CREATE-relations point to or from accounts we consider DApp accounts, accounts at the other end of the CREATE-relation are added to the known set of DApp accounts
            - Redo the loop for newly identified account addresses
            --> Keep all the trace data, because it is helpful for process mining
        
        So what the loop does is: 
        This loop performs the following key steps for each contract address:
        1. Fetches transactions involving the contracts based on normal transactions, internal transactions, and transactions with contract events (based on the configuration).
        2. Concatenates the fetched transactions into a single DataFrame, df_txs_lx, for further processing.
        3. Removes duplicate transactions and filters out previously processed transactions.
        4. Retrieves transaction traces and updates the trace_tree DataFrame. trace_tree collects all trace data.
        5. Identifies relevant CREATE-relations in the transactions.
        6. Updates the list of contract addresses and uses new contract addresses for the next iteration.

        The loop continues until there are no more new contract addresses added to the list of contract addresses left in set_contracts_lx.

        Exceptions:
            - Raises ValueError if no transaction data is fetched in an iteration, indicating a potential issue or an empty dataset.

        Note:
            - The loop will exit if df_txs_lx is empty after fetching transactions, indicating no further data is available.
            - Several files are created fr debugging, but not necessarily saved, unless uncommented, e.g.:
                - non_verified_addresses: addresses for which no ABI could be retrieved from Etherscan, because they are not verified (at best, standard events / predefined functions can be decoded for those)
                - verified_addresses
                - txs_event_not_decoded_dapp
                - unknown_event_addresses_dapp
        """
        
        logger.info("##### GETTING TRANSACTIONS #####")

        df_txs_lx = pd.DataFrame()
        
        # Get normal transactions
        if extract_normal_transactions:
            try:
                df_txs_lx_normal = get_transactions.get_transactions(set_contracts_lx, min_block, max_block, internal_flag="normal", etherscan_api_key=etherscan_api_key)
                df_txs_lx = pd.concat([df_txs_lx, df_txs_lx_normal])
            except Exception as e:
                logger.error(f"Error fetching normal transactions: {e}")
        
        # Get internal transactions
        if extract_internal_transactions:
            try:
                df_txs_lx_internal = get_transactions.get_transactions(set_contracts_lx, min_block, max_block, internal_flag="internal", etherscan_api_key=etherscan_api_key)
                df_txs_lx = pd.concat([df_txs_lx, df_txs_lx_internal])
            except Exception as e:
                logger.error(f"Error fetching internal transactions: {e}")

        # Get transactions by events
        if extract_transactions_by_events:
            try:
                df_txs_lx_events = get_transactions.get_transactions_by_events(node_url, set_contracts_lx, min_block, max_block)
                df_txs_lx = pd.concat([df_txs_lx, df_txs_lx_events])
            except Exception as e:
                logger.error(f"Error fetching transactions by events: {e}")

        # Check if DataFrame is empty
        if df_txs_lx.empty:
            raise ValueError("No transaction data fetched. Ensure at least one data extraction type is selected.")
        
        # df_txs_lx = pd.concat([df_txs_lx_normal, df_txs_lx_internal, df_txs_lx_events])

        # Remove duplicates from df_txs_lx
        df_txs_lx.drop_duplicates(subset='hash', keep="last", inplace=True)

        # Consider only transactions that have not already been looked at
        df_txs_lx = df_txs_lx[~df_txs_lx["hash"].isin(list_txs_all)]
        
        ### Breaking the loop ###
        # If no additional txs were found, the search is over
        if df_txs_lx.empty == True:
            logger.info("No additional transactions were found. The data extraction ends as planned.")
            break
        
        logger.info(f"Number of new transactions for the next iteration: {len(df_txs_lx)}")
        # Reindex, to be able to loop later
        df_txs_lx.reset_index(drop=True, inplace=True)

        # Update the set of all transactions that have been included to be able to check in the next iteration, if the trace for that transactios still has to be created, or not (if it was created already, it is in this list)
        list_txs_all.update(df_txs_lx["hash"].tolist())

        logger.info("##### COMPUTING TRACES #####")        
        
        # Create traces of newly added transactions
        df_trace_lx = trace_transformation.tx_to_trace(df_txs_lx, node_url)
        df_trace_tree = pd.concat([df_trace_tree, df_trace_lx], axis=0)
        
        logger.info("SUCCESS: Transactions were retrieved and traces were recomputed and transformed.")

        logger.info("STARTING: Identifing relevant CREATE-relations.")
        contracts_dapp, set_contracts_lx = create_relations.create_relations(df_trace_tree, contracts_dapp, set_contracts_lx, set_predefined_non_dapp_contracts)
            
        logger.info(f"LIST OF NEW CONTRACTS LEVEL {level} COUNT: {len(set_contracts_lx)}")

        level = level+1
        
        logger.info(f"THIS WAS LOOP {level-1}")
        
        time.sleep(1)

    # Save data as CSV and as pickle file
    path = os.path.join(dir_path, "resources", "df_trace_tree_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".csv")
    df_trace_tree.to_csv(path)
    path = os.path.join(dir_path, "resources", "df_trace_tree_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
    df_trace_tree.to_pickle(path) 
    # pickle.dump(df_trace_tree, open(dir_path+r'\resources\df_trace_tree_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl', 'wb'))

    logger.info(f"Total number of extracted operations: {len(df_trace_tree)}")
    
    # free up memory
    del df_trace_tree
    del df_trace_lx
    del df_txs_lx
    del df_txs_lx_internal
    del df_txs_lx_normal

    # Save list of contracts as TXT and as pickle file to have a list of contract belonging to the DApp available
    path = os.path.join(dir_path, "resources", "contracts_dapp_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".txt")
    with open(path, "w") as output:
        output.write(str(contracts_dapp))
    path = os.path.join(dir_path, "resources", "contracts_dapp_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
    pickle.dump(contracts_dapp, open(path, 'wb'))

    logger.info("DONE WITH THE DATA EXTRACTION")

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
    
    logger.info("STARTING TO DECODE DATA")

    logger.info("Starting to ingest data.")
    # Ingest a dataframe with trace data to decode
    path = os.path.join(dir_path, "resources", "df_trace_tree_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
    df_log = pickle.load(open(path, 'rb'))

    # With Pandas, in case the ingestion goes wrong at some point
    # df_log = pd.read_pickle(open(dir_path+r'\resources\df_trace_tree_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl', 'rb')) 

    logger.info("Done ingesting data.")

    # Ingest dapp contracts
    path = os.path.join(dir_path, "resources", "contracts_dapp_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
    contracts_dapp = pickle.load(open(path, 'rb'))

    df_log = data_preparation.base_transformation(df_log, contracts_dapp)

    # only events in the traces have the attribute "address", so selecting contract addresses in which events occurred == selecting entries of the attribute "address"
    try: 
        addresses_events = df_log["address"].unique()
        addresses_events = list(addresses_events)
    except: 
        logger.debug("Selecting contracts with events failed. No contracts with events?")
        addresses_events = list()
    # only CALLs have the characteristic "CALL" in the attribute "calltype"
    # The call came from outside the contract INTO the contract, so the contract address is in the attribute "to"
    try:  
        addresses_calls = df_log[df_log["calltype"] == "CALL"]["to"].unique()
        addresses_calls = list(addresses_calls)
    except: 
        logger.debug("Selecting contracts with CALLs failed. No contracts with CALLs?")
        addresses_calls = list()
    # Only DELEGATECALLs have the characteristic "DELEGATECALL" in the attribute "calltype"
    # The call came from outside the contract INTO the contract, so the contract address is in the attribute "to" 
    try: 
        addresses_delegatecall = df_log[df_log["calltype"] == "DELEGATECALL"]["to"].unique()
        addresses_delegatecall = list(addresses_delegatecall)
    except:
        logger.debug("Selecting contracts with events failed. No contracts with DELEGATECALLs?")
        addresses_delegatecall = list()

    # Make one list of all addresses with relevant entries
    try:
        addresses = addresses_events + addresses_calls + addresses_delegatecall
    except:
        logger.debug("Creating a full list of contracts failed.")

    # Keep unique values
    addresses = set(addresses)
    # Remove NaNs from the list of addresses
    # https://stackoverflow.com/questions/21011777/how-can-i-remove-nan-from-list-python-numpy
    try:
        addresses = [x for x in addresses if str(x) != 'nan']
    except: 
        logger.debug("Removing NaN values from the list of addresses failed.")
    
    # Remove capitalized letters. If everything is always lower-case, it's easier to ensure that string comparisons work (because 'a'!='A')
    logger.debug("Removing capital letters.")
    addresses = list(map(helpers.low, addresses))

    logger.info(f"{len(addresses_events)} contracts with events, {len(addresses_calls)} contracts with CALLs, {len(addresses_delegatecall)} contracts with DELEGATECALLs, {len(addresses)} unique contracts to look up ABIs for.")

    # Decoding requires extra information, e.g., event specifications, function specifications
    # That data is available in the ABIs of the contracts
    # So next, the ABIs are retrieved from Etherscan (if possible) 
    logger.info("Starting to retrieve ABIs.")
    
    if (non_dapp_decode_events == False) and (non_dapp_decode_calls_with_ether_transfer == False) and (non_dapp_decode_calls_with_no_ether_transfer == False) and (non_dapp_decode_delegatecalls == False):
        logger.info("Only retrieving DApp CA ABIs. Decoding of non-DApp CAs disabled in config-file.")
        addresses = list(set(addresses) & set(contracts_dapp))
    
    dict_abi, non_verified_addresses, verified_addresses = data_preparation.create_abi_dict(addresses, etherscan_api_key)

    # Intermediate saving of the ABIs and df_log, as both will not change, but something might go wrong down the line
    #path = os.path.join(dir_path, "resources", "addresses_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
    #pickle.dump(addresses, open(path, 'wb'))

    path = os.path.join(dir_path, "resources", "dict_abi_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
    pickle.dump(dict_abi, open(path, 'wb'))


    """
    The following lines have a repeating pattern: 
        - Create a mask and select all addresses that are also part of contracts_dapp, to ensure only DApp account related events are picked
        (Or if non-DApp: Create a mask and select all addresses that are NOT part of contracts_dapp, to ensure only NON-DApp account related events are picked)
        - For CALL / DELEGATECALL, specify the variable calltype_list (e.g., calltype_list = ["CALL"])
        - Decode the data (pick the right function / module)
        - Save the decoded data as CSV and as pickle-files. 
        - There is the option to save additional files for debugging, incl.: 
            - a list of transactions for which the event could not be decoded
            - a list of addresses for which the event could not be decoded
    """

    ######################################## DAPP ########################################

    #################### EVENTS ####################

    ##### EVENTS DAPP #####
    if dapp_decode_events == True:
        logger.info("Starting to decode EVENTS of DApp contracts")
        
        file_name_snipped = "df_events_dapp_"
        
        mask_dapp = df_log["address"].isin(contracts_dapp)
        df_events_dapp = df_log[mask_dapp]
        
        df_events_dapp, txs_event_not_decoded_dapp, unknown_event_addresses_dapp = data_preparation.decode_events(df_events_dapp, dict_abi)

        # Save the decoded events as CSV and as pickle. 
        # There is the option to save additional files for debugging, incl.: 
        #   - a list of transactions for which the event could not be decoded
        #   - a list of addresses for which the event could not be decoded
        path = os.path.join(dir_path, "resources", file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".csv")
        df_events_dapp.to_csv(path)
        path = os.path.join(dir_path, "resources", file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
        pickle.dump(df_events_dapp, open(path, 'wb'))
        #path = os.path.join(dir_path, "resources", "txs_event_not_decoded_dapp_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
        #pickle.dump(txs_event_not_decoded_dapp, open(path, 'wb'))
        #path = os.path.join(dir_path, "resources", "unknown_event_addresses_dapp_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
        #pickle.dump(unknown_event_addresses_dapp, open(path, 'wb'))

        # free up memory
        del df_events_dapp 

    if dapp_decode_events == False:
        logger.info("Skipping EVENTS of DApp contracts. Reason: 'false' flag in config file.")
        

    #################### CALLS ####################


    ##### CALLs DAPP WITH ETHER TRANSFER #####
    if dapp_decode_calls_with_ether_transfer == True: 
        logger.info("Start decoding CALLs with Ether transfer to DApp contracts")

        file_name_snipped = "df_call_dapp_with_ether_transfer_"
        logging_string = "DAPP WITH ETHER TRANSFER"
                
        mask_dapp = df_log["to"].isin(contracts_dapp)
        df_functions_dapp = df_log[mask_dapp]

        # Exclude zero value transactions
        zero_value_flag = False
        calltype_list = ["CALL"]
        df_functions_dapp, addresses_not_dapp, txs_function_not_decoded, addresses_noAbi = data_preparation.decode_functions(df_functions_dapp, dict_abi, node_url, calltype_list, zero_value_flag, logging_string)

        path = os.path.join(dir_path, "resources", file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".csv")
        df_functions_dapp.to_csv(path)
        path = os.path.join(dir_path, "resources", file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
        pickle.dump(df_functions_dapp, open(path, 'wb'))

        # free up memory
        del df_functions_dapp

    if dapp_decode_calls_with_ether_transfer == False: 
        logger.info("Skipping CALLs with Ether transfer to DApp contracts. Reason: 'false' flag in config file.")

    ##### CALLs DAPP WITH NO ETHER TRANSFER #####    
    if dapp_decode_calls_with_no_ether_transfer == True: 
        logger.info("Start decoding CALLs with no Ether transfer to DApp contracts")
        
        file_name_snipped = "df_call_dapp_with_no_ether_transfer_"
        logging_string = "DAPP WITH NO ETHER TRANSFER"
        
        mask_dapp = df_log["to"].isin(contracts_dapp)
        df_functions_dapp = df_log[mask_dapp]

        # Exclude zero value transactions
        zero_value_flag = True
        calltype_list = ["CALL"]
        df_functions_dapp, addresses_not_dapp, txs_function_not_decoded, addresses_noAbi = data_preparation.decode_functions(df_functions_dapp, dict_abi, node_url, calltype_list, zero_value_flag, logging_string)

        path = os.path.join(dir_path, "resources", file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".csv")
        df_functions_dapp.to_csv(path)
        path = os.path.join(dir_path, "resources", file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
        pickle.dump(df_functions_dapp, open(path, 'wb'))

        # free up memory
        del df_functions_dapp

    if dapp_decode_calls_with_no_ether_transfer == False: 
        logger.info("Skipping CALLs with no Ether transfer to DApp contracts. Reason: 'false' flag in config file")

    #################### DELEGATECALLs ####################
    ##### DELEGATECALL DAPP #####
    if dapp_decode_delegatecalls == True:
        logger.info("Starting to decode DELEGATECALLs to DApp contracts")

        file_name_snipped = "df_delegatecall_dapp_"
        logging_string = "DAPP"

        mask_dapp = df_log["to"].isin(contracts_dapp)
        df_functions_dapp = df_log[mask_dapp]

        zero_value_flag = False
        calltype_list = ["DELEGATECALL"]
        df_delegate_dapp, addresses_not_dapp, txs_function_not_decoded, addresses_noAbi = data_preparation.decode_functions(df_functions_dapp, dict_abi, node_url, calltype_list, zero_value_flag, logging_string)

        path = os.path.join(dir_path, "resources", file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".csv")
        df_delegate_dapp.to_csv(path)
        path = os.path.join(dir_path, "resources", file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
        pickle.dump(df_delegate_dapp, open(path, 'wb'))

        # free up memory
        del df_delegate_dapp

    if dapp_decode_delegatecalls == False:
        logger.info("Skipping DELEGATECALLs to DApp contracts. Reason: 'false' flag in config file.")

    #path = os.path.join(dir_path, "resources", "addresses_not_dapp_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
    #pickle.dump(addresses_not_dapp, open(path, 'wb'))
    #path = os.path.join(dir_path, "resources", "txs_function_not_decoded_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
    #pickle.dump(txs_function_not_decoded, open(path, 'wb'))


        
    ##### CREATED CONTRACTS ##### 

    logger.info("Starting to save CREATE-relations")

    mask_create = df_log["calltype"].isin(["CREATE", "CREATE2"])
    df_creations = df_log[mask_create]
    path = os.path.join(dir_path, "resources", "df_creations_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".csv")
    df_creations.to_csv(path)
    path = os.path.join(dir_path, "resources", "df_creations_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
    pickle.dump(df_creations, open(path, 'wb'))

    # free up memory
    del df_creations

    ######################################## NON-DAPP ########################################
    ##### EVENTS NON-DAPP #####
    if non_dapp_decode_events == True:
        logger.info("Starting to decode EVENTS of NON-DApp contracts")
        
        file_name_snipped = "df_events_non_dapp_"

        mask_dapp = df_log["address"].isin(contracts_dapp)
        df_events_non_dapp = df_log[~mask_dapp]

        df_events_non_dapp, txs_event_not_decoded_non_dapp, unknown_event_addresses_non_dapp = data_preparation.decode_events(df_events_non_dapp, dict_abi)

        path = os.path.join(dir_path, "resources", file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".csv")
        df_events_non_dapp.to_csv(path)
        path = os.path.join(dir_path, "resources", file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
        pickle.dump(df_events_non_dapp, open(path, 'wb'))
        #path = os.path.join(dir_path, "resources", "txs_event_not_decoded_non_dapp_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
        #pickle.dump(txs_event_not_decoded_non_dapp, open(path, 'wb'))
        #path = os.path.join(dir_path, "resources", "unknown_event_addresses_non_dapp" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
        #pickle.dump(unknown_event_addresses_non_dapp, open(path, 'wb'))

        # free up memory
        del df_events_non_dapp

    if non_dapp_decode_events == False:
        logger.info("Skipping EVENTS of NON-DApp contracts. Reason: 'false' flag in config file.")

    ##### CALLs NON DAPP WITH ETHER TRANSFER #####
    if non_dapp_decode_calls_with_ether_transfer == True:
        logger.info("Starting to decode CALLs with Ether transfer to NON-DApp contracts")
        
        file_name_snipped = "df_call_with_ether_transfer_non_dapp_"
        logging_string = "NON DAPP WITH ETHER TRANSFER"

        mask_dapp = df_log["to"].isin(contracts_dapp)
        df_functions_non_dapp = df_log[~mask_dapp]

        zero_value_flag = False
        calltype_list = ["CALL"]
        df_functions_non_dapp, addresses_not_dapp, txs_function_not_decoded, addresses_noAbi = data_preparation.decode_functions(df_functions_non_dapp, dict_abi, node_url, calltype_list, zero_value_flag, logging_string)

        path = os.path.join(dir_path, "resources", file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".csv")
        df_functions_non_dapp.to_csv(path)
        path = os.path.join(dir_path, "resources", file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
        pickle.dump(df_functions_non_dapp, open(path, 'wb'))

        # free up memory
        del df_functions_non_dapp

    if non_dapp_decode_calls_with_ether_transfer == False:
        logger.info("Skipping CALLs with Ether transfer to NON-DApp contracts. Reason: 'false' flag in config file.")

    ##### CALLs NON DAPP WITH NO ETHER TRANSFER #####
    if non_dapp_decode_calls_with_no_ether_transfer == True:
        logger.info("Starting to decode CALLs with no Ether transfer to NON-DApp contracts")
        
        file_name_snipped = "df_call_with_no_ether_transfer_non_dapp_"
        logging_string = "NON DAPP WITH NO ETHER TRANSFER"

        mask_dapp = df_log["to"].isin(contracts_dapp)
        df_functions_non_dapp = df_log[~mask_dapp]

        zero_value_flag = True
        calltype_list = ["CALL"]
        df_functions_non_dapp, addresses_not_dapp, txs_function_not_decoded, addresses_noAbi = data_preparation.decode_functions(df_functions_non_dapp, dict_abi, node_url, calltype_list, zero_value_flag, logging_string)

        path = os.path.join(dir_path, "resources", file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".csv")
        df_functions_non_dapp.to_csv(path)
        path = os.path.join(dir_path, "resources", file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
        pickle.dump(df_functions_non_dapp, open(path, 'wb'))

        # free up memory
        del df_functions_non_dapp

    if non_dapp_decode_calls_with_no_ether_transfer == False:
        logger.info("Skipping CALLs with no Ether transfer to NON-DApp contracts. Reason: 'false' flag in config file.")

    ##### DELEGATECALLs NON DAPP #####
    if non_dapp_decode_delegatecalls == True:
        logger.info("Starting to decode DELEGATECALLs to NON-DApp contracts")
        logging_string = "NON DAPP"

        mask_dapp = df_log["to"].isin(contracts_dapp)
        df_functions_non_dapp = df_log[~mask_dapp]

        zero_value_flag = False
        calltype_list = ["DELEGATECALL"]
        df_delegate_non_dapp, addresses_not_dapp, txs_function_not_decoded, addresses_noAbi = data_preparation.decode_functions(df_functions_non_dapp, dict_abi, node_url, calltype_list, zero_value_flag, logging_string)

        path = os.path.join(dir_path, "resources", "df_delegatecall_non_dapp_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".csv")
        df_delegate_non_dapp.to_csv(path)
        path = os.path.join(dir_path, "resources", "df_delegatecall_non_dapp_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
        pickle.dump(df_delegate_non_dapp, open(path, 'wb'))

        # free up memory
        del df_delegate_non_dapp
        
    if non_dapp_decode_delegatecalls == False:
        logger.info("Skipping DELEGATECALLs to NON-DApp contracts. Reason: 'false' flag in config file.")
    #################### LOG CONSTRUCTION ####################
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



