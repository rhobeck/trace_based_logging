import numpy as np
import pandas as pd
from . import event_decoder
# import trace_decoder.event_decoder as event_decoder
import datetime
import time
import requests
import json
from web3 import Web3, HTTPProvider
import os
from src.trace_based_logging.logging_config import setup_logging



logger = setup_logging()

MAX_API_RETRIES = 5

# change the default look-up path to the directory above
dir_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..'))

def load_event_definitions(config_file):
    with open(config_file, 'r') as file:
        event_definitions = json.load(file)
    return event_definitions

def low(x):
    return x.lower()

# TODO: if contracts_dapp not provided, load it. Little tricky: contracts_dapp is a set provided in the config, but there might ba a longer version saved in a previous run
# Good opportunity to move loading data like df_log into a seperate function.

def base_transformation(df_log, contracts_dapp, config, state):
    """
    Performs basic data transformations on a DataFrame of blockchain logs. 
    This includes resetting the index, separating and cleaning 'from' and 'address' fields from ordering attachments,
    formatting timestamps, converting addresses to lowercase, and renaming columns to avoid conflicts.

    Args:
        df_log (pd.DataFrame): A DataFrame containing blockchain log entries. Expected to have 'from', 'address',
                               and 'timeStamp' columns among others.
        contracts_dapp (set or list): A collection of contract addresses that are identified as belonging to the DApp.

    Returns:
        pd.DataFrame: The transformed DataFrame with cleaned and formatted data suitable for further analysis.

    Detailed Operations:
        - Resets the DataFrame index for continuity.
        - Separates order IDs from 'from' and 'address' fields, if appended, and calculates a combined order of execution.
        - Cleans up the 'from' and 'address' fields by removing any ordering attachment.
        - Formats 'timeStamp' to a datetime object from a unix timestamp and ensures proper datetime formatting.
        - Converts 'to', 'from', and 'address' fields to lowercase to maintain consistency.
        - Renames 'type' and 'value' columns to 'calltype' and 'callvalue' respectively to avoid naming conflicts.
        - Flags entries where the 'address' field matches any of the DApp contract addresses.
        - Prints timestamps indicating the start and completion of the data transformation process.

    Notes:
        - The function explicitly expects 'from', 'address', and 'timeStamp' fields in the input DataFrame.
        - The transformation process is designed for preparation of log data for further processing and analysis,
          particularly in identifying DApp-related activities.
        - The 'timeStamp' field is initially converted to an integer, formatted to a string representation of a datetime,
          and then converted back to a datetime object for consistency in handling date and time information.
        - Addresses are converted to lowercase to ensure uniformity in address representation.
    """
    logger.info("Start basic data transformation.")
    
    
    if not isinstance(df_log, pd.DataFrame):
        logger.info("The function input df_log must be a pandas DataFrame. Trying to load the DataFrame from a pickle or CSV file.")
        
        dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

        base_contract = state["base_contract"]
        csv_path = os.path.join(dir_path, "resources", f"df_trace_tree_{base_contract}_{config['min_block']}_{config['max_block']}.csv")
        pkl_path = os.path.join(dir_path, "resources", f"df_trace_tree_{base_contract}_{config['min_block']}_{config['max_block']}.pkl")

        # Try to load the DataFrame
        if os.path.exists(pkl_path):
            df_log = pd.read_pickle(pkl_path)
            logger.info("Loaded DataFrame from pickle file.")
        elif os.path.exists(csv_path):
            df_log = pd.read_csv(csv_path)
            logger.info("Loaded DataFrame from CSV file.")
        else:
            raise FileNotFoundError("Neither the pickle file nor the CSV file exists.")

    if not isinstance(contracts_dapp, (list, set)):
        raise ValueError("The function input contracts_dapp must be a pandas DataFrame")

    required_columns = ['from', "to", 'address', 'timeStamp']
    missing_columns = [col for col in required_columns if col not in df_log.columns]
    if missing_columns:
        raise ValueError(f"The input DataFrame is missing required columns: {', '.join(missing_columns)}")


    df_log.reset_index(drop=True, inplace=True)

    try:
        # df_txs_lx["timeStamp"] = df_txs_lx["timeStamp"].apply(lambda x: int(x))
        # seperating addresses and order IDs
        # df_log["order_calls"] = df_log["from"].apply(lambda x: int(x[43:]) if isinstance(x, str) else 0)
        # df_log["order_events"] = df_log["address"].apply(lambda x: int(x[43:]) if isinstance(x, str) else 0)
        # df_log["order_in_trace"] = df_log["order_calls"] + df_log["order_events"]
        # df_log.drop(["order_calls", "order_events"], axis=1, inplace=True)

        # delete the ordering attachement from "from" and "address"
        # df_log["from"] = df_log["from"].apply(lambda x: x[:42] if isinstance(x, str) else np.nan)
        # df_log["address"] = df_log["address"].apply(lambda x: x[:42] if isinstance(x, str) else np.nan)

        # timestamp formatting got lost
        if "timeStamp" in df_log.columns:
            df_log["timeStamp"] = df_log["timeStamp"].apply(lambda x: int(x))
            df_log["timeStamp"] = df_log["timeStamp"].apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%d.%m.%Y  %H:%M:%S.%f'))
            df_log["timeStamp"] = df_log["timeStamp"].apply(lambda x: datetime.datetime.strptime(x, '%d.%m.%Y  %H:%M:%S.%f'))
        else: 
            logger.info("Base transformation: No 'timeStamp' found in the data.")
        # use the order to attach the int as milliseconds to the timestamp, so that mining algorithm can create order by timestamp
        # df_log["milliseconds"] = pd.to_timedelta(df_log["order"], unit="ms")
        # df_log["timeStamp_ordered"] = df_log["timeStamp"] + df_log["milliseconds"]

        # df_log["timeStamp"] = df_log["timeStamp"].apply(lambda x: x.strftime('%d.%m.%Y  %H:%M:%S.%f'))
        # df_log["timeStamp_ordered"] = df_log["timeStamp_ordered"].apply(lambda x: x.strftime('%d.%m.%Y  %H:%M:%S.%f'))

        # lower case for addresses
        df_log["to"] = df_log["to"].apply(lambda x: str(x).lower())
            
        df_log["from"] = df_log["from"].apply(lambda x: str(x).lower())
        
        df_log["address"] = df_log["address"].apply(lambda x: str(x).lower() if str(x) != "nan" else x)

        # rename column "type" because some function and event attributes might have the same name, that count lead to problems with concatenating dataframes
        # same for "value"
       
        df_log.rename(columns={"type": "calltype"}, inplace=True)
        df_log.rename(columns={"value": "callvalue"}, inplace=True)
        
        # keep list entries as list entries, not as string
        # https://stackoverflow.com/questions/20799593/reading-csv-containing-a-list-in-pandas
        # df_log["topics"] = df_log["topics"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else np.nan)
        #df_log["topics"] = df_log["topics"].apply(lambda x: x.tolist()[0] if str(x) != "nan" else x)
        
        df_log = convert_hex_to_int(df_log)
    
    except Exception as e:
        logger.error(f"Error during data transformation: {e}")
        raise
    
    
    logger.info("Done with basic data transformation.")
    
    return df_log


def address_selection(df_log):
    # only events in the traces have the attribute "address", so selecting contract addresses in which events occurred == selecting entries of the attribute "address"
    try: 
        if "address" in df_log.columns:
            addresses_events = df_log["address"].dropna().unique().tolist()
            logger.debug("ADDRESSES EVENTS: ", addresses_events)
    except: 
        logger.debug("Selecting contracts with events failed.")
        addresses_events = list()
    # only CALLs have the characteristic "CALL" in the attribute "calltype"
    # The call came from outside the contract INTO the contract, so the contract address is in the attribute "to"
    try:  
        if "calltype" in df_log.columns:
            addresses_calls = df_log[df_log["calltype"] == "CALL"]["to"].dropna().unique().tolist()
    except: 
        logger.debug("Selecting contracts with CALLs failed.")
        addresses_calls = list()
    # Only DELEGATECALLs have the characteristic "DELEGATECALL" in the attribute "calltype"
    # The call came from outside the contract INTO the contract, so the contract address is in the attribute "to" 
    try: 
        if "calltype" in df_log.columns:
            addresses_delegatecall = df_log[df_log["calltype"] == "DELEGATECALL"]["to"].dropna().unique().tolist()
    except:
        logger.debug("Selecting contracts with events failed.")
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
    addresses = list(map(low, addresses))

    logger.info(f"{len(addresses_events)} contracts with events, {len(addresses_calls)} contracts with CALLs, {len(addresses_delegatecall)} contracts with DELEGATECALLs, {len(addresses)} unique contracts to look up ABIs for.")

    return addresses#, addresses_events, addresses_calls, addresses_delegatecall


def convert_hex_to_int(df_log, list_of_cols=["gas", "gasUsed", "callvalue"]):
    """
    Converts hex values in a DataFrame to integers.
    """
    def safe_hex_to_int(value):
        # Convert hex values to integers, retaining original value on error."
        try:
            return int(value, 16) if pd.notna(value) else value
        except ValueError:
            return value

    for col in list_of_cols:
        if col not in df_log.columns:
            logger.warning(f"Column '{col}' does not exist in DataFrame. Skipping this column.")
            continue  
            
        try:
            df_log[col] = df_log[col].apply(safe_hex_to_int)
        except Exception as e: 
        # Alternatively a non-specific exception if this does not work
            logger.error(f"Error processing column {col}: {e}; retained original value")

    logger.info(f"Hex values converted (or retained) for columns {list_of_cols}")

    return df_log


def create_abi_dict(addresses, etherscan_api_key):
    """
    Retrieves the ABI (Application Binary Interface) for a list of contract addresses from Etherscan and categorizes them
    into verified and non-verified based on the availability of their source code.

    Args:
        addresses (list or set): A collection of contract addresses for which to retrieve the ABIs.

    Returns:
        tuple: A tuple containing three elements:
            - dict_abi (dict): A dictionary mapping contract addresses to their ABIs for contracts with verified source code.
            - non_verified_addresses (set): A set containing contract addresses without verified source code.
            - verified_addresses (set): A set containing contract addresses with verified source code.

    Overview:
        The function iterates through each provided contract address, making up to three attempts to retrieve the ABI from
        the Etherscan API. If the source code for a contract address is not verified, the address is added to the 
        `non_verified_addresses` set. If the source code is verified, the ABI is parsed from the API response and stored 
        in the `dict_abi` dictionary with the contract address as the key. The function also logs the progress of ABI 
        retrieval, including the number of addresses processed and the count of verified versus non-verified addresses.

    Notes:
        - The function assumes that the Etherscan API's response structure is consistent and that an ABI can be directly
          parsed from the `result` field of a successful API response.
        - Error handling is implemented via retry logic, with a brief pause between attempts. However, the function logs
          a generic message upon failure without raising an exception, which might require refinement for better error 
          management and reporting.
    """

    if not isinstance(addresses, list):
        raise ValueError("The function input addresses must be a list.")

    dict_abi = {}
    non_verified_addresses = set()
    verified_addresses = set()
    f = 0
    
    for contract_address_tmp in addresses: 
        attempts = 0
        # Three attempts to retrieve the ABI from the Etherscan API
        while attempts < MAX_API_RETRIES:
            try: 
                api_key = etherscan_api_key
                headers = {'Content-type': 'application/json'}
                parameters = {
                    "module": "contract",
                    "action": "getabi",
                    "address": contract_address_tmp,
                    "apikey": api_key
                }    
                response_API = requests.get('https://api.etherscan.io/api', parameters, headers=headers)
                response_json = response_API.json()
                break
            # Inexplicit exception
            except:
                attempts += 1
                
                logger.error(f"{str(attempts)} attempt(s) failed. Was the library 'requests' imported? {contract_address_tmp}. Retrying...")
                
                time.sleep(2)
                response_json = {"result:"}
        
        # check if the key "result" is in the JSON response, if not retry, then skip
        
        # if the address has no verified source code, save the address 
        if response_json["result"] == "Contract source code not verified":
            f += 1
            non_verified_addresses.add(contract_address_tmp)
        # if the address has verified source code, save the ABI in a dictionary
        else:
            abi = json.loads(response_json["result"])
            # verified_addresses is not necessary, as those addresses are also saved in the dict_abi keys
            verified_addresses.add(contract_address_tmp)
            dict_abi[contract_address_tmp] = abi
        
        logger.info(f"ABI dictionary: {len(verified_addresses)+len(non_verified_addresses)} of {len(addresses)} addresses. Number of valid ABIs: {len(dict_abi)}")
    
    logger.info(f"{len(dict_abi)} contract ABI(s) retrieved. {f} contract(s) without verified ABI(s)")
    
    return dict_abi#, non_verified_addresses, verified_addresses

def decode_events(df_log, dict_abi):
    """
    Decodes blockchain event data using the ABI definitions provided. This function processes a DataFrame of 
    log entries, extracting and decoding event data for each entry based on the contract ABI.

    Args:
        df_log (pd.DataFrame): A DataFrame containing log entries from blockchain transactions. It must include 
                               columns for 'address', 'topics', and 'data', representing the contract address, 
                               event topics, and event data, respectively.
        dict_abi (dict): A dictionary mapping contract addresses to their respective ABI (Application Binary Interface)
                         definitions. Addresses must be lowercase hex strings.

    Raises:
        ValueError: If the inputs `df_log` and `dict_abi` are not in the expected format (DataFrame for `df_log` and 
                    dictionary for `dict_abi`).

    Returns:
        pd.DataFrame: A DataFrame with decoded event data. Each row corresponds to a blockchain event, with columns 
                      for event parameters and values, alongside existing trace data from `df_log`.
        list: A list of transaction hashes that could not be decoded due to missing or incorrect ABI data.
        set: A set of contract addresses for which events could not be decoded, indicating either missing ABIs in 
             `dict_abi` or events not covered by the ABI definitions provided.

    Overview:
        The function first filters `df_log` for entries with non-null 'address' values, indicating potential event 
        logs. For each of these entries, it attempts to decode the event data using the contract's ABI from `dict_abi`
        and the event's 'topics' and 'data'. The decoded event data, along with selected information from the original 
        log entry, is compiled into a new DataFrame of decoded events.

        In cases where an event cannot be decoded (due to missing ABI data, incorrect ABI data, or other errors), the 
        function logs the issue and includes the original log entry in the output DataFrame, marking it as not decoded.
    """
    
    if not isinstance(df_log, pd.DataFrame):
        raise ValueError("The function input df_log must be a pandas DataFrame")

    if not isinstance(dict_abi, dict):
        raise ValueError("The function input dict_abi must be a pandas DataFrame")

    # Select data with a address. These can only be blockchain events (and not CALLs / DELEGATECALLs / CREATEs; those do not have the attribute "address") 
    # The selected data will be decoded. 
    df_events_raw = df_log[~df_log["address"].isna()]
    df_events_raw.reset_index(drop = True, inplace = True)

    # free up memory
    del df_log
    
    unknown_event_count = 0
    
    max_row = len(df_events_raw)

    unknown_event_addresses = set()
    txs_event_not_decoded = list()

    # load fallback_abis
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    path = os.path.join(dir_path, 'config_custom_events.json')
    fallback_abis = load_event_definitions(path)

    logger.info(f"Starting to decode events, number of entries: {len(df_events_raw)}")

    # Each event already has a row assigned in df_events_raw. The encoded data is accessed by looping through the dataframe. 
    accumulated_data = []
    for index, row in df_events_raw.iterrows():
        address = row["address"]
        topics = row["topics"]
        data = row["data"]
        try: 
            
            # Here the decoding happens
            decoded_event = event_decoder.event_decoder(address, topics, data, dict_abi, fallback_abis)
            
            # "name" will be the name of the event
            name = decoded_event["name"]
            address = decoded_event["address"]
            colNames = list()
            values = list()
            
            # The goal is a tabular format with event paramenters as columns and event parameter values as row entries
            # Events can have a varying number of parameters. Loop through them. 
            
            for j in range(len(decoded_event["data"])):
                colName = decoded_event["data"][j]["name"]
                value = decoded_event["data"][j]["value"]
                colNames.append(colName)
                values.append(value)

            # Existing data in the dataframe df_events_raw (e.g., on the trace) is reused in the new dataframe
            trace_data_values = [
                row["timeStamp"], row["tracePos"], row["tracePosDepth"],
                row["hash"], row["blockNumber"], row["transactionIndex"] 
                ]
            trace_data_colNames = ["timeStamp", "tracePos", "tracePosDepth", "hash", "blockNumber", "transactionIndex"]

            # Create a list with the data that was available
            # 1. now decoded name of the event parameter as column name
            # 2. now decoded event attribute data as future row entries
            data = [name, address]
            data.extend(trace_data_values)
            data.extend(values)

            # Create a list with the column names
            columns = ["name", "address"]
            columns.extend(trace_data_colNames)            
            
            # In case of duplicate column names (e.g., a column name already in use for transaction data like "hash" is also used as an event parameter / column name)
            # To avoid duplicates, change the respective column name for the event parameter 
            
            list_overlap = set(columns) & set(colNames)
            for element in colNames:
                if element in list_overlap:
                    index_to_change = colNames.index(element)
                    colNames[index_to_change] = element + "_eventAttribute"
            columns.extend(colNames)
            
            # Create a dictionary with column names a keys and data as values
            # This can later be formatted as a row in the dataframe with all the trace data
            row_data = dict(zip(columns, data))
            accumulated_data.append(row_data)

        # Not so ideal error handling
        except NameError:
            logger.debug("Failed to build topic map")
            txs_event_not_decoded.append(row["hash"])
            data = list(row)
            columns = list(df_events_raw.columns.values)
            row_data = dict(zip(columns, data))
            accumulated_data.append(row_data)
            unknown_event_count += 1
        except:
            unknown_event_addresses.add(row["address"])
            unknown_event_count += 1
            logger.debug(f"Event topic is not present in given ABIs {row['hash']}")
            txs_event_not_decoded.append(row["hash"])        
            columns = list(df_events_raw.columns.values)
            data = list(row)
            row_data = dict(zip(columns, data))
            accumulated_data.append(row_data)

        #ts = datetime.datetime.fromtimestamp(time.time()).strftime('%d-%m-%Y %H:%M:%S')
        #print(ts, "Event decoding: ", index, " out of ", len(df_events_raw), " events decoded.")
        
        # Intermediate logging in 10% steps of the loop range to report on progress
        if index != 0:
            state = round(max_row, -1) / index
            if state in [100/10,100/20,100/30,100/40,100/50,100/60,100/70,100/80,100/90]:
                logger.info(f"Events decoding: {index} out of {max_row} events decoded.")
            
    logger.info(f"Events decoding: DONE. {unknown_event_count} unknown events occurred. Now building dataframe.")
    
    # free up memory
    del df_events_raw
    
    df_events = pd.DataFrame(accumulated_data)
    if "address" in df_events.columns:
        df_events["address"] = df_events["address"].str.lower()

      
    return df_events#, txs_event_not_decoded, unknown_event_addresses


# TODO: decoding ABIs could be decoded once in one go, and the function could be isolated 

def decode_functions(df_log, dict_abi, node_url, calltype_list, include_zero_value_transactions, logging_string):
    """
    Decodes function call data from Ethereum transaction logs using the contract ABIs. It filters transactions based
    on specified call types and non-zero Ether transfer values, then attempts to decode each transaction's input data.

    Args:
        df_log (pd.DataFrame): DataFrame containing Ethereum transaction logs with columns for transaction data such as
                               'calltype', 'callvalue', 'input', etc.
        dict_abi (dict): A dictionary mapping contract addresses (as lowercase hex strings) to their respective ABIs.
        node_url (str): The URL of the Ethereum node used to connect via Web3.
        calltype_list (list): A list of call types (e.g., ['CALL', 'DELEGATECALL']) to filter the transactions by.

    Raises:
        ValueError: If input types for `df_log`, `dict_abi`, or `calltype_list` are not as expected.

    Returns:
        pd.DataFrame: A DataFrame containing decoded function call data along with original transaction data.
        set: A set of contract addresses for which no ABI was available, indicating they could not be processed.
        list: A list of transaction hashes that could not be decoded.
        set: A set of contract addresses deemed not to be part of the DApp based on the decoding process.

    Overview:
        The function processes each transaction in the filtered DataFrame, attempting to decode its input data using
        the ABI associated with its contract address. Successfully decoded transactions are compiled into a new DataFrame
        alongside relevant transaction data. Transactions that cannot be decoded due to missing ABIs or other errors are
        tracked separately.

    Note:
        This function assumes that the 'callvalue' column is used to filter transactions of interest based on the
        presence of value transfers. Transactions with a 'callvalue' of "0x0" are excluded from the decoding process.
        The function also normalizes contract addresses to lowercase to match the keys in `dict_abi` and relies on
        the external `process_abi` function to initialize Web3 contract objects.
    
    TODO: Improve error handling
    """

    if not isinstance(df_log, pd.DataFrame):
        raise ValueError("df_log must be a pandas DataFrame")

    if not isinstance(dict_abi, dict):
        raise ValueError("dict_abi must be a pandas DataFrame")

    if not isinstance(calltype_list, list):
        raise ValueError("calltype_list must be a pandas DataFrame")

    # Select data with the calltype picked as an input parameter (CALL / DELEGATECALL)
    # Select only data with a callvalue (Ether transfer value). The assumption is that only data with a value transfer is interesting
    # Note that there is a lot if callvalue == 0 calls. They can also be decoded but for use cases chosen so far its just too much for an off-the shelf laptop to handle (in the current implementation)
    # The selected data will be decoded. 
    mask = df_log["calltype"].isin(calltype_list)
    mask_zero = df_log["callvalue"].apply(lambda x: x == 0.0 or x == "0x0")
    if include_zero_value_transactions:
        # Select only rows with zero callvalue
        mask_callvalue = mask_zero
    else:
        # Exclude rows with zero callvalue
        mask_callvalue = ~mask_zero
        
    df_function_raw = df_log[mask & mask_callvalue]
    
    # free up memory
    del df_log 
    # df_function_raw = df_log[mask & (df_log["callvalue"] != "0x0") & (pd.notna(df_log["callvalue"]))]

    df_function_raw.reset_index(drop=True, inplace=True)

    addresses_noAbi = set()
    addresses_not_dapp = set()
    txs_function_not_decoded = list()
    unknown_functions_count = 0

    logger.info(f"ABI decoding: {len(dict_abi)} ABIs to decode.")
    
    # Prepare contract objects and format the ABI data for decoding 
    contract_objects = {}
    for address in dict_abi.keys():
        try: 
            address = address.lower()
            abi = dict_abi[address]
            contract_objects[address.lower()] = process_abi(abi, address, node_url)
        except Exception as e: 
            logger.error(f"Failed to process ABI for address {address}: {e}")
            addresses_noAbi.add(address)

    logger.info(f"Function decoding starting for {str(calltype_list)}, number of entries: {len(df_function_raw)}")
    
    max_row = len(df_function_raw)
    start = time.time()
    accumulated_data = []


    for index, row in df_function_raw.iterrows():
        
        
        # Intermediate logging in 10% steps of the loop range to report on progress
        if index != 0:
            state = round(max_row, -1) / index
            if state in [100/10,100/20,100/30,100/40,100/50,100/60,100/70,100/80,100/90]:
                logger.info(f"Function decoding: {logging_string} {str(calltype_list)}, {index} out of {max_row} function calls decoded. Decoding failed for {unknown_functions_count}")

        
        # Select the address of the contract the function was called in ("to") and the function input data to decode ("input")
        contract_address_tmp = row["to"]
        contract_address_tmp = contract_address_tmp
        input_data = row["input"]

        # Get ABIs from the dictionary of ABIs (only lower case)
        try: 
            contract = contract_objects.get(contract_address_tmp)
        except:
            logger.debug("Contract not found in ABIs., {index}")
            addresses_not_dapp.add(contract_address_tmp)
            txs_function_not_decoded.append(row["hash"])
            data = list(row)
            columns = list(df_function_raw.columns.values)
            row_data = dict(zip(columns, data))
            accumulated_data.append(row_data)
            unknown_functions_count += 1
            continue
        
        try:
            # The actual decoding happens here.
            # func_obj is the name of the function and its abstract parameters, func_params are the instantiated parameters
            func_obj, func_params = contract.decode_function_input(input_data)
        
        except:
            logger.debug(f"Function parameters could not be decoded for contract {contract_address_tmp} in transaction {row['hash']}, {index}")
            addresses_not_dapp.add(contract_address_tmp)
            txs_function_not_decoded.append(row["hash"])
            data = list(row)
            columns = list(df_function_raw.columns.values)
            row_data = dict(zip(columns, data))
            accumulated_data.append(row_data)
            unknown_functions_count += 1
            continue
            
        
        try:
            # The decoded data will be prepared for a tabular structure. So column names and row entries are needed. 
            # func_params is a dict with parameters (column names) as keys and values as values.
            # The column names need some processing. The values not.
            colNames = list()
            values = list()
            for item in func_params:
                colName = item
                # remove initial "_" in func_params
                if colName[0] == "_":
                    colName = colName[1:]
                # Conflicts in later concatenation can arise from duplicated column names that already describe transaction data (e.g., "from" and "to") 
                # To avoid such duplicates, known transaction parameters are edited
                # TODO: Add other known transaction parameters (see right below)
                if colName == "to":
                    colName = "to_function_internal"
                if colName == "from":
                    colName = "from_function_internal"
                value = func_params[item]
                colNames.append(colName)
                values.append(value)

            # use data from existing DataFrame
            trace_data_values = [row["from"],  row["to"], 
                                    row["gas"], row["gasUsed"],
                                    row["output"], row["callvalue"],
                                    row["calltype"], row["hash"],
                                    row["timeStamp"], row["tracePos"], 
                                    row["tracePosDepth"], row["blockNumber"],
                                    row["transactionIndex"]
                                    ]
            trace_data_colNames = ["from", "to", "gas", "gasUsed", "output", "callvalue", "calltype", "hash", "timeStamp", "tracePos", "tracePosDepth", "blockNumber", "transactionIndex"]

            # Create a list with the data that is available 
            # 1. name of the object as function name
            # 2. trace data from the that was not encoded
            # 3. newly decoded data
            data = [str(func_obj)] # func_obj is an object, but we only need the function name as string
            data.extend(trace_data_values)
            data.extend(values)

            # Create a list with column names
            # 1. default label for the function name
            # 2. names for known columns from not encoded trace data
            # 3. names for decoded columns (without duplicates)
            columns = ["name"]
            columns.extend(trace_data_colNames)            

            # In case of duplicate column names, change the respective column names
            list_overlap = set(columns) & set(colNames)
            for element in colNames:
                if element in list_overlap:
                    index_to_change = colNames.index(element)
                    colNames[index_to_change] = element + "_functionAttribute"
            columns.extend(colNames)
            
            # Create a dictionary with column names a keys and data as values
            # This can later be formatted as a row in the dataframe with all the trace data
            row_data = dict(zip(columns, data))
            accumulated_data.append(row_data)
            logger.debug(f"SUCCESS {contract_address_tmp} in transaction {row['hash']}, {index}")
        except:
            logger.debug(f"Data transformation after decoding failed for contract {contract_address_tmp}, {index}")
            addresses_not_dapp.add(contract_address_tmp)
            txs_function_not_decoded.append(row["hash"])
            data = list(row)
            columns = list(df_function_raw.columns.values)
            row_data = dict(zip(columns, data))
            accumulated_data.append(row_data)
            unknown_functions_count += 1 
            continue
                    
    logger.info(f"Function decoding: DONE. Total function calls {len(df_function_raw)}. Undecoded function calls {unknown_functions_count}. Now building dataframe.")
    
    # free up memory
    del df_function_raw
#    path = os.path.join(dir_path, "resources", 'accumulated_data_calls_zero_value.pkl')
#    pickle.dump(accumulated_data, open(path, "wb"))
    

    
    
    # Build the dataframe in 5 segments
    total_length = len(accumulated_data)

    # Calculate split indices to divide data into 5 equal parts
    split_indices = [int(total_length * i / 5) for i in range(6)]  # Generates indices from 0 to total_length

    df_segments = []  # List to store DataFrame segments

    for i in range(5):
        start_index = split_indices[i]
        end_index = split_indices[i + 1]
        segment_data = accumulated_data[start_index:end_index]
        segment_df = pd.DataFrame(segment_data)
        df_segments.append(segment_df)  # Append segment to the list

    # Concatenate all segments at once
    df_function = pd.concat(df_segments, ignore_index=True)

    # Free up memory
    del df_segments
    del accumulated_data
    

    # reset index for masking
    df_function.reset_index(drop=True, inplace=True) 
    
    # Make the lists a tabular format
    # df_function = pd.DataFrame(accumulated_data)

    end = time.time()
    logger.debug(f"Time lapsed for decoding *CALL data {(end-start)}")
    return df_function#, addresses_not_dapp, txs_function_not_decoded, addresses_noAbi

def process_abi(abi, contract_address_tmp, node_url):
    """
    Processes ABI data for a given contract address and initializes a contract object using Web3.

    Args:
        abi (str or dict): The ABI of the contract, either as a JSON string or a dictionary.
        contract_address_tmp (str): The contract address in hex format.
        node_url (str): The URL of the Ethereum node to connect to.

    Returns:
        web3.eth.Contract: A Web3 contract object initialized with the given ABI and address.

    Raises:
        ValueError: If the ABI or contract address is invalid.
        ConnectionError: If there is a problem connecting to the node URL.
    """
    try: 
        # format abi output, there are some symbols making the output harder to process
        abi = str(abi)
        abi = abi.replace("'", "\"")
        abi = abi.replace(" ", "")
        abi = abi.replace("False", "false")
        abi = abi.replace("True", "true")

        w3 = Web3(Web3(HTTPProvider(node_url)))
        if not w3.isConnected():
                raise ConnectionError(f"Unable to connect to node at {node_url}")

        # format addresses to "checksum" (some letters are capitalized)
        address_checksum = Web3.toChecksumAddress(contract_address_tmp)
        #address_checksum = address_checksum.lower()
        #    try:
        contract = w3.eth.contract(address=address_checksum, abi=abi)
        return contract
    except ValueError as e:
        logger.error(f"Invalid contract address {contract_address_tmp}: {e}")
        raise ValueError("Invalid contract address.")
    except ConnectionError as e:
        logger.error(f"Connection error with node {node_url}: {e}")
        raise ConnectionError("Problem connecting to the Ethereum node.")
    except Exception as e:
        logger.error(f"Unexpected error processing ABI for {contract_address_tmp}: {e}")
        raise Exception("Unexpected error during ABI processing.")
