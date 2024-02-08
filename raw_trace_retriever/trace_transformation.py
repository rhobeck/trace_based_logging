import pandas as pd
import time
import datetime
import requests
import math
from logging_config import setup_logging

"""
This module provides functionalities for interacting with blockchain nodes to retrieve and process transaction trace data. 
It includes functions for fetching JSON data for transaction hashes, converting transaction data into trace data, 
flattening nested JSON structures, and handling nested transactions.

Functions:
    json_retriever(tx_hash, node_url, max_attempts=15):
        Retrieves JSON data for a given transaction hash from a blockchain node, with a specified number of retry attempts.

    tx_to_trace(df_txs_lx, node_url):
        Converts a DataFrame of transactions into a DataFrame of transaction traces by retrieving trace data from a blockchain node.

    insert_order(trace_json_lx, start):
        Recursively appends an order number to the values of specific keys ('from', 'address') in a trace JSON structure.

    flatten(trace_json_lx, new_trace_json_lx={}):
        Flattens a nested dictionary by recursively merging its nested structures into the parent dictionary.

    flatten_nested(df_flat_json_tmp, df_trace_l1, tx_hash, functionName, timestamp, blockNumber):
        Flattens nested JSON arguments in a DataFrame column named "calls" (if it exists) and organizes the data in a tabular format.

    explode_df(df_flat_json, nested_cols):
        Helper function to further flatten and explode nested columns ('calls', 'logs') in a DataFrame obtained from JSON data.

Constants:
    INCREMENT_FACTOR (int): A constant used to determine the batch size for processing transactions in chunks.

Variables:
    invalid_tx_hash (set): A set used to store transaction hashes that resulted in invalid or unexpected JSON responses.
"""

logger = setup_logging()

INCREMENT_FACTOR = 100

invalid_tx_hash = set([])

def json_retriever(tx_hash, node_url, max_attempts=15):
    """
    Retrieves JSON data for a given transaction hash from a blockchain node.

    Args:
        tx_hash (str): The transaction hash to retrieve the trace for.
        node_url (str): The URL of the blockchain node to query.
        max_attempts (int): Maximum number of attempts for the request.

    Returns:
        tuple: A tuple containing the JSON response and a boolean flag indicating success.
    """
    headers = {'Content-type': 'application/json'}
    parameters = {
        "jsonrpc": "2.0",
        "method": "debug_traceTransaction",
        "params": [tx_hash, {"tracer": 'callTracer', "tracerConfig": {"withLog": True}}],
        "id": 1
    }
    attempts = 0
    json_flag = True

    while attempts < max_attempts:
        try:
            response = requests.post(node_url, json=parameters, headers=headers)
            # Check if response is valid and contains JSON
            response_json = response.json()  # This line could raise ValueError if response is not valid JSON
            if response_json and "result" in response_json and isinstance(response_json["result"], dict) and "type" in response_json["result"]:
                return response_json, json_flag
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception for tx_hash {tx_hash}: {e}")
        except ValueError as e:
            logger.error(f"JSON decoding error for tx_hash {tx_hash}: {e}")

        attempts += 1
        if attempts == max_attempts:
            ts = datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            logger.error(f"{ts} Max attempts reached. Invalid tx hash: {tx_hash}.")
            json_flag = False

    return {}, json_flag

"""
def json_retriever(tx_hash, node_url):

    headers = {'Content-type': 'application/json'}
    parameters = {
        "jsonrpc": "2.0",
        "method": "debug_traceTransaction",
        #"params": [tx_hash, {"tracer": 'callTracer'}],
        "params": [tx_hash, {"tracer": 'callTracer', "tracerConfig": { "withLog": True }} ],
        "id": 1
    }
    # Retrieve CALL data from the node
    # Some requests are not processed properly; we give 15 attempts to retrieve a proper JSON response
    # Retrieve CALL data from the node
    attempts=0
    json_flag = True
    while True:
        try:
            response = requests.post(node_url, json=parameters, headers=headers)
            # if the following two items are available, the response is in healthy format
            response.json()["result"]["type"]
            break
        except Exception:
            attempts += 1
            if attempts==max_attempts:
                ts = datetime.datetime.fromtimestamp(time.time()).strftime('%d-%m-%Y %H:%M:%S')
                print(ts, "Exception: Unexpected JSON response. Invalid tx hash: ", tx_hash, " Best to recreate the JSON-trace and debug.")
                
                invalid_tx_hash.add(tx_hash)
                # unexpected_response.append(response.json())
                json_flag = False
                break
    return response.json(), json_flag

"""

def tx_to_trace(df_txs_lx, node_url):
    """
    Converts a DataFrame of transactions into a DataFrame of transaction traces by retrieving trace data from a blockchain node. 
    This function iterates over each transaction in the input DataFrame, retrieves its trace data from the specified node,
    processes the trace data, and compiles the results into a new DataFrame containing the trace information.

    Args:
        df_txs_lx (pd.DataFrame): A DataFrame containing transactions. Each transaction is expected to have at least
                                  the 'hash', 'timeStamp', and 'blockNumber' fields.
        node_url (str): The URL of the Ethereum node from which to retrieve trace data. This URL should point to an 
                        API endpoint capable of returning transaction trace data (have Geth debug functionality, e.g., Geth or Erigon).

    Returns:
        pd.DataFrame: A DataFrame containing the traces of the transactions. This includes processed and flattened
                      trace data for each transaction in `df_txs_lx`. The DataFrame is expanded to include detailed
                      trace information such as execution order and potentially nested trace details, depending on the
                      structure of the returned trace JSON data.

    Overview:
        The function performs the following steps for each transaction in `df_txs_lx`:
        1. Retrieves the transaction hash and other relevant details from the input DataFrame.
        2. Makes a request to the specified node URL to fetch the trace data for each transaction hash.
        3. Processes the retrieved trace data to insert execution order and flatten the JSON structure.
        4. Compiles the processed trace data into a temporary DataFrame, which is then concatenated to the final
           DataFrame of transaction traces.
        5. Prints progress updates and timing information to the console.

    Note:
        - This function contains a placeholder for the 'functionName' variable, which is currently not dynamically 
          retrieved, as far as I know it is not used down the line.
        - It uses an 'INCREMENT_FACTOR' (defined at the start of the module) to determine the batch size for 
          processing transactions in chunks. 
        - The function includes a method for appending execution order to the "from" and "address" fields in the
          trace JSON which needs refinement.
        - Error handling within the function relies on a 'json_flag' returned by 'json_retriever' to skip transactions
          with faulty JSON data without halting the entire process.
        - The loop should be implemented with, e.g., iterrows() to loop efficiently.
    """
    data = []
    df_trace_lx = pd.DataFrame(data)
    tic = time.time()
    
    c_max = len(df_txs_lx)
    c_tmp = 1
    c_tmp_minus = 0
    loop_round = 0
    increment = math.ceil(c_max / INCREMENT_FACTOR)
    while c_tmp_minus < c_max: 
        df_trace_lx_tmp = pd.DataFrame()
        loop_round += 1 
    
        for i in range(c_tmp_minus, c_tmp):
            tx_hash=str(df_txs_lx.iloc[i]['hash'])
            functionName="Place holder"#str(df_txs_lx.iloc[i]['functionName'])
            timestamp=df_txs_lx.iloc[i]['timeStamp']
            blockNumber = df_txs_lx.iloc[i]['blockNumber']
            
            if (i != 0 and i % 100 == 0) or i == len(df_txs_lx)-1:
                logger.info(f"Transactions for which traces were retrieved: {i+1}")

            # retrieve JSON data
            trace_json_lx, json_flag = json_retriever(tx_hash, node_url)
            # json_flag in case something was wrong with the JSON from the server (i.e., json_flag == false), the respective tx hash is skipped. 
            # corresponding faulty hash is logged already in json_retriever
            if json_flag == False:
                continue
            # insert order of execution; NOTE: THIS NEEDS REFINEMENT, order is appended to "from" and "address"
            trace_json_lx = insert_order(trace_json_lx, start = [0])
            # flatten the JSON data
            df_flat_json = pd.DataFrame.from_dict(flatten(trace_json_lx, {}), orient="index").T
            # flatten nested JSON data
            df_trace_lx_tmp = flatten_nested(df_flat_json, df_trace_lx_tmp, tx_hash, functionName, timestamp, blockNumber)       

        logger.info(f"{c_tmp} transactions of {len(df_txs_lx)} transactions; loop number: {loop_round}")

        c_tmp_minus = c_tmp
        c_tmp+=increment
        df_trace_lx = pd.concat([df_trace_lx, df_trace_lx_tmp], axis=0)
        if c_tmp > c_max:
            c_tmp = c_max
    
    toc = time.time()

    logger.debug(f"SPEEDTEST: Time to recompute and transform transaction traces: {toc - tic}")
    logger.info(f"LENGTH OF TRACE DF: {len(df_trace_lx)}")

    df_trace_lx.reset_index(inplace=True, drop=True)
    
    return df_trace_lx


def insert_order(trace_json_lx, start):  
    """
    Recursively appends an order number to the values of specific keys ('from', 'address') in a trace JSON structure.

    Args:
        trace_data (dict | list): The trace data in the form of a dictionary or list containing nested dictionaries.
        order (list): The current order number to append. Default is 0 and increments with each recursive call.
                      In hindside I am not sure why the order is determined via the length of a list. Needs revision.

    Returns:
        dict | list: The modified trace data with order numbers appended to specified keys.
    """
    key_strings = ["from", "address"]
    for key, value in trace_json_lx.items():
        if key in key_strings:
            # passing a single variable in the recursion caused copying of the variable inside the instead of updating to the latest value
            # so the latest value was appended to a list of which the last element was printed
            # if enumeration by depth of the the JSON file makes more sense: make 'start' an int variable and do start += 1 instead of latest += 1
            latest=start[len(start)-1]
            start.append(latest + 1)
            #print(key, start[len(start)-1])
            trace_json_lx[key]= str(value) + "_" + str(start[len(start)-1])
        if isinstance(value, list):
            for nested_value in value:
                if isinstance(nested_value, str) or isinstance(nested_value, int):
                    pass
                else:
                    insert_order(nested_value, start)
        if isinstance(value, dict):
            insert_order(value, start)
    return trace_json_lx

# Input: JSON file with nested data containing the calls.
# Goal: DataFrame with call-type, sender and receiver
# https://stackoverflow.com/questions/72633357/how-to-identify-and-explode-a-nested-json-file-as-columns-of-a-dataframe


def flatten(trace_json_lx, new_trace_json_lx={}):
    """
    Flattens a nested dictionary by recursively merging its nested structures into the parent dictionary.

    This function traverses a nested dictionary (trace_json_lx) and transfers all key-value pairs to a new, 
    initially empty dictionary (new_trace_json_lx). If a value is a dictionary itself, the function recurses 
    into it to flatten its contents as well. 

    Args:
        trace_json_lx (dict): The nested dictionary to be flattened. This dictionary represents a trace JSON
                              structure that potentially contains other dictionaries as values for some of its keys.
        new_trace_json_lx (dict, optional): The dictionary into which the flattened content is merged. This 
                                            parameter allows the function to build up the flattened result across 
                                            recursive calls. It defaults to an empty dictionary when the function 
                                            is initially called.

    Returns:
        dict: A dictionary with the same content as trace_json_lx, but flattened. 
    """
    for key, value in trace_json_lx.items():
        if isinstance(value, dict):
            flatten(value, new_trace_json_lx)
        if isinstance(value, str) or isinstance(value, int) or isinstance(value, list):
            new_trace_json_lx[key] = value
    return new_trace_json_lx

# Alternatively (but missing the first internal transaction):
# df_normalized_from_json = pd.json_normalize(trace_json_l1["result"]["calls"])


def flatten_nested(df_flat_json_tmp, df_trace_l1, tx_hash, functionName, timestamp, blockNumber):
    """Function to flatten nested JSON arguments in a dataframe column named "calls" (if it exists)
    Args:
        DataFrame with nested JSON arguments
    Output:
        DataFrame with data organized table like only (nothing nested)
    """

    # check if there are any internal transactions ("calls" appear in the flattened JSON)
    # https://stackoverflow.com/questions/26577516/how-to-test-if-a-string-contains-one-of-the-substrings-in-a-list-in-pandas
#    nested_cols = ['calls', 'topics', 'logs']
    nested_cols = ['calls', 'logs']
    # boolean_list = df_flat_json_tmp.columns.str.contains('|'.join(nested_cols))
    # if there are internal transactions, fetch them
    df_trace_l1_tmp = df_flat_json_tmp.copy()
    #i = 0
    # as long as the dataframe contains columns that are known to be nested, execute the following routine
    while True in df_flat_json_tmp.columns.str.contains('|'.join(nested_cols)):
        # Unnest the nested columns
        # Note that explode_df() creates a dataframe on the basis of the contents of nested columns, i.e., if additional nested columns ("calls" or "logs") only appear in the exploded dataframe if they are further nested 
        df_explode_tmp=explode_df(df_flat_json_tmp, nested_cols) 
        df_flat_json_tmp = df_explode_tmp
        df_trace_l1_tmp = pd.concat([df_trace_l1_tmp, df_explode_tmp], axis=0)
        # Once nested columns no longer appear in the dataframe, stop the loop
        #boolean_list = df_flat_json_tmp.columns.str.contains('|'.join(nested_cols))

    # Remove columns with non-helpful information
    if "index" in df_trace_l1_tmp.columns.values.tolist():
        df_trace_l1_tmp.drop(["index"], inplace=True, axis=1)
    if "jsonrpc" in df_trace_l1_tmp.columns.values.tolist():
        df_trace_l1_tmp.drop(["jsonrpc"], inplace=True, axis=1)
    if "id" in df_trace_l1_tmp.columns.values.tolist():
        df_trace_l1_tmp.drop(["id"], inplace=True, axis=1)
    # The columns "calls" and "logs" did hold sections of the JSON-dictionary that were to be unfolded during "explode_df()". The two colums are no longer needed, as the unfolded information is now available in the dataframe
    if "calls" in df_trace_l1_tmp.columns.values.tolist():
        df_trace_l1_tmp.drop(["calls"], inplace=True, axis=1)
    if "logs" in df_trace_l1_tmp.columns.values.tolist():
        df_trace_l1_tmp.drop(["logs"], inplace=True, axis=1)
        
    df_trace_l1_tmp["hash"]=tx_hash
    df_trace_l1_tmp["functionName"]=functionName
    df_trace_l1_tmp["timeStamp"]=timestamp
    df_trace_l1_tmp["blockNumber"]=blockNumber
    
    df_trace_l1 = pd.concat([df_trace_l1, df_trace_l1_tmp], axis=0)
    return df_trace_l1


def explode_df (df_flat_json, nested_cols):
    """Helper function.

     Args:
         df_flat_json: DataFrame with content of the original JSON file flattened on first level but with nested column entries

     Returns:
        Flattened dataframe on level +1.
    """
    df_out = pd.DataFrame([])
    
    for item in nested_cols:
        # if the item in qustion is contained do ...
        if True in df_flat_json.columns.str.contains(item):
            dfe = df_flat_json.explode(item)
            # look for cells that have non-exploded data (are not empty)
            # access the cells, access the data, make it an array and attach it to the dataframe
            mask = dfe[item].isna()
            df = pd.DataFrame([i for i in dfe[~mask][item].to_numpy()]).reset_index(drop=True)
            df_out = pd.concat([df_out, df], axis=0)
        else: 
            df_out=df_out

    return df_out