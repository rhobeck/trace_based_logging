import time
import pandas as pd
import requests
import datetime
from web3 import Web3
import math
from logging_config import setup_logging


"""
This module provides functionalities to fetch and process blockchain transaction data from Etherscan (normal and internal transactions) and a blockchain node via Web3 (blockchain events). It supports fetching normal transactions, internal transactions, and transactions by events for specified Ethereum contracts within a given block range.

Functions:
- send_api_request: Sends a request to the Etherscan API to fetch transaction data.
- request_mediator: Processes the JSON response from Etherscan, converting it into a DataFrame.
- get_transactions: Fetches and processes transactions for a list of contracts.
- get_transactions_by_events: Fetches transactions by querying contract events and extracting the transaction hashes.
- event_fetcher: Fetches logs of events from a blockchain node using Web3.

Constants:
- CHUNK_SIZE: Defines the chunk size for processing blocks in batches.
- DELAY: Specifies the delay between API requests to manage server load and avoid throttling.
"""

# The function get_transactions_by_events() works in chunks; here is the definition of the chunk size
CHUNK_SIZE = 10000
# At times, recovery times for servers between requests make sense; here is the definition of the recovery time
DELAY = 0.02


# Create a logger
logger = setup_logging()

####################### internal - external #######################

def send_api_request(contract_address_tmp, min_block, max_block, internal_flag, etherscan_api_key):
    """
    Sends an API request to Etherscan to fetch transaction data for a given contract address within a specified block range.

    Args:
        contract_address_tmp (str): The contract address for which transactions are to be fetched.
        min_block (int): The starting block number for the query range.
        max_block (int): The ending block number for the query range.
        internal_flag (str): Specifies the type of transactions to fetch ('normal' or 'internal').
        etherscan_api_key (str): The API key for accessing Etherscan's API service.

    Returns:
        requests.Response: The response object from the Etherscan API request.

    Raises:
        Exception: If the API request fails after the specified number of retries.
    """
    api_key = etherscan_api_key
    
    max_retries = 5
        
    # we are interested in 1) txs with the contract as sender / receiver ("normal" txs) and 2) txs the contract was part of (on Etherscan called "internal transactions"). To differentiate the requests, there is a flag
    if internal_flag == "normal":
        parameters = {
            "module": "account",
            "action": "txlist",
            "address": contract_address_tmp,
            "startblock": min_block,
            "endblock": max_block, 
            "apikey": api_key
        }    
    if internal_flag == "internal":
        parameters = {
            "module": "account",
            "action": "txlistinternal",
            "address": contract_address_tmp,
            "startblock": min_block,
            "endblock": max_block, 
            "apikey": api_key
        }
    
    for attempt in range(max_retries):
        
        try:
            response_API = requests.get('https://api.etherscan.io/api', parameters)
            return response_API
        except requests.exceptions.ConnectionError as e:
            logger.error(f"ConnectionError encountered: {e}. Retrying...")
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTPError encountered: {e}. Retrying...")
        except requests.exceptions.RequestException as e:
            logger.error(f"Other RequestException encountered: {e}. Retrying...")
        time.sleep(DELAY)
    raise Exception(f"API request failed after {max_retries} attempts")




def request_mediator(txs_json_lx, df_txs_lx, contract_address_tmp): 
    """
    Processes the JSON response from Etherscan API, converting transaction data into a pandas DataFrame and concatenating it with existing transaction data.

    Args:
        txs_json_lx (dict): The JSON response containing transaction data from Etherscan.
        df_txs_lx (pd.DataFrame): The existing DataFrame of transaction data to which new data will be appended.
        contract_address_tmp (str): The contract address for which the transaction data was fetched.

    Returns:
        tuple: A tuple containing:
            - df_txs_lx (pd.DataFrame): Updated DataFrame with new transaction data appended.
            - df_txs_lx_tmp (pd.DataFrame): DataFrame containing only the new transaction data.
    """
    
    try:
        if txs_json_lx["status"] == "1" :
            df_txs_lx_tmp = pd.DataFrame(txs_json_lx["result"],columns=txs_json_lx["result"][0].keys())
            df_txs_lx = pd.concat([df_txs_lx, df_txs_lx_tmp], axis=0)
        else:
            ts = datetime.datetime.fromtimestamp(time.time()).strftime('%d-%m-%Y %H:%M:%S')
            logger.debug(f"0 transactions for the contract: {contract_address_tmp}")
            df_txs_lx_tmp = pd.DataFrame()
    except Exception:
        logger.error(f"Exception occurred while extracting txs from Etherscan and formatting them. Contract: {contract_address_tmp}; The contract address is ignored, the extraction continues.")
        # So that number of transactions can be 0 
        df_txs_lx_tmp = pd.DataFrame()
        pass 
    
    return df_txs_lx, df_txs_lx_tmp#, count_txs_tmp


def get_transactions(list_lx, min_block, max_block, internal_flag, etherscan_api_key):
    """
    Fetches and processes transactions for a list of contract addresses based on the specified type ('normal' or 'internal').

    Args:
        list_lx (list): A list of contract addresses.
        min_block (int): The starting block number for the query range.
        max_block (int): The ending block number for the query range.
        internal_flag (str): Specifies the type of transactions to fetch ('normal' or 'internal').
        etherscan_api_key (str): The API key for accessing Etherscan's API service.

    Returns:
        pd.DataFrame: A DataFrame containing all fetched and processed transactions for the specified contracts.
    """
    
    if internal_flag not in ["normal", "internal"]:
        logger.error(f"Unexpected internal_flag value: {internal_flag}; Set the flag for transaction requests on Etherscan to 'normal' or 'internal'")
    if internal_flag == "normal":
        string_snipped = "NORMAL"
    if internal_flag == "internal":
        string_snipped = "INTERNAL"
    i=0
    min_block_fix = min_block
# Careful: max 1000 transactions per contract with es.get_transactions_by_address(), therefore API request
    df_txs_lx=pd.DataFrame()
    count_txs = 0
    for contract_address_tmp in list_lx:
        
        # Retry loop for retrieving transactions from Etherscan
        for attempt in range(5): 
            try:
                try: 
                    response_API = send_api_request(contract_address_tmp, min_block, max_block, internal_flag, etherscan_api_key) 
                except Exception as e:
                    logger.error(f"Failed to connect to Etherscan to retrieve transactions: {e}")
                
                txs_json_lx = response_API.json()
                
                df_txs_lx, df_txs_lx_tmp = request_mediator(txs_json_lx, df_txs_lx, contract_address_tmp)

                logger.info(f"{string_snipped} transactions for {i+1} / {len(list_lx)} contracts received. Total count of {string_snipped} transactions: {len(df_txs_lx)} {contract_address_tmp}")

            # Etherscan returns <= 10 000 transactions at one request. If more are available, they have to be requested again. 
                while len(df_txs_lx_tmp) == 10000:
                    # Maximum number of transactions per block is ~400 txs (2023), i.e., <10,000 txs.
                    # When number of received txs == 10,000 there is a high chance only a fraction of available txs of the last considered block was returned from Etherscan. 
                    # Hence, the final block of the last iteration has to be the first block of the next iteration (to get all transactions within the block, NOT min_block_new = min_block_old+1)
                    df_txs_lx_tmp['blocknumber'] = pd.to_numeric(df_txs_lx_tmp.blockNumber)
                    min_block = df_txs_lx_tmp.blocknumber.max()
                    
                    try: 
                        response_API = send_api_request(contract_address_tmp, min_block, max_block, internal_flag, etherscan_api_key) 
                    except Exception as e:
                        logger.error(f"Failed to connect to Etherscan to retrieve transactions: {e}")
                    
                    txs_json_lx = response_API.json()
                    
                    df_txs_lx, df_txs_lx_tmp = request_mediator(txs_json_lx, df_txs_lx, contract_address_tmp)

                    logger.info(f"{string_snipped} transactions for {i+1} / {len(list_lx)} contracts received. Total count of {string_snipped} transactions: {len(df_txs_lx)} {contract_address_tmp}")
                i = i+1
                # for the next contract in the iteration, the min_block has to be reset to the original block range minimum
                min_block = min_block_fix 
            # retry if exception occurred, TODO: more specific exception
            except:
                logger.error(f"Transactions could not be received for {contract_address_tmp}. Unspecific error. Retrying ...")
                time.sleep(DELAY)
                continue
            # break the retry loop
            else:
                break
        # skip contract if all retries failed
        else:
            logger.error(f"Transaction could not be received for {contract_address_tmp}. Contract is skipped.")
            time.sleep(DELAY)
            continue
        
    # There will be transaction duplicates, duplicates are not necessary, so they are removed
    df_txs_lx.drop_duplicates(subset='hash', keep="last", inplace=True)
    
    # Several unnecessary columns are dropped to save memory
    if internal_flag == "normal" and len(df_txs_lx) != 0:
        df_txs_lx.drop(["nonce", "blockHash", "transactionIndex", "from", "to", "value", "gas", 'gasPrice', 'isError', 'txreceipt_status', 'input', "cumulativeGasUsed", 'gasUsed', 'confirmations', 'methodId', 'functionName'], axis=1, inplace=True)
    if internal_flag == "internal" and len(df_txs_lx) != 0:
        df_txs_lx.drop(["from", "to", "value", "gas", 'isError', 'input', 'gasUsed', 'type', 'traceId', 'errCode'], axis=1, inplace=True)
    
    return df_txs_lx


def get_transactions_by_events(node_url, contracts, min_block, max_block):
    
    """
    Fetches transactions related to specified contracts by listening to their events within a given block range.

    Args:
        node_url (str): The URL of the Ethereum node to connect to via Web3.
        contracts (list): A list of contract addresses to listen for events.
        min_block (int): The starting block number for the query range.
        max_block (int): The ending block number for the query range.

    Returns:
        pd.DataFrame: A DataFrame containing transactions fetched based on contract events.
    """
    
    # Chunking / batching was implemented with ChatGPT and then debugged

    logger.info("Starting to get transactions by events.")
    # ts = datetime.datetime.fromtimestamp(time.time()).strftime('%d-%m-%Y %H:%M:%S')
    # print(ts, "Start getting transactions by events.")
    w3 = Web3(Web3.HTTPProvider(node_url))
    log_data_list = []

    for contract_address in contracts:
        # Iterate in chunks
        for chunk_start in range(min_block, max_block+1, CHUNK_SIZE):
            chunk_end = min(chunk_start + CHUNK_SIZE - 1, max_block)

            # Define the filter criteria
            parameters = {
                'address': Web3.toChecksumAddress(contract_address),
                'fromBlock': chunk_start,
                'toBlock': chunk_end,
            }
            
            try: 
                logs = event_fetcher(parameters, w3) 
            except Exception as e:
                logger.error(f"Failed to connect to the blockchain node to retrieve events: {e}")
                
            logger.debug(f"Number of events: {len(logs)} between {chunk_start} and {chunk_end} for the contract: {contract_address}")
            
           
            i = 1
            previous_blockNumber = None
            for log in logs:
                blockNumber = log['blockNumber']
                
                # to insert the time into the log data, a block object has to be created. To save time, a new block element is only created if the blockNumber changed
                if blockNumber != previous_blockNumber:
                    block = w3.eth.get_block(blockNumber)

                previous_blockNumber = blockNumber
                
                log_data = {
                    'blockNumber': blockNumber,
                    "timeStamp": block["timestamp"],
                    'hash': log['transactionHash'].hex(),
                    'contractAddress': log['address']
                }
                
                log_data_list.append(log_data)
                
            # logger.debug(f"Done with building the list.")
            
        logger.info(f"EVENT-BASED transactions for contracts {list(contracts).index(contract_address)+1} / {len(contracts)} Total count of transactions: {len(log_data_list)} Block: {chunk_end} / {max_block} {contract_address}")
            # ts = datetime.datetime.fromtimestamp(time.time()).strftime('%d-%m-%Y %H:%M:%S')
            # print(ts, "EVENT-BASED transactions for contracts", list(contracts).index(contract_address)+1, "/", len(contracts), " Count of transactions:", len(log_data_list), "Block: ", chunk_end, "/", max_block, " ", contract_address)

    df = pd.DataFrame(log_data_list)
    return df

def event_fetcher(parameters, w3):
    """
    Fetches logs of events for a given set of parameters using Web3.

    Args:
        parameters (dict): A dictionary of parameters for the log query, including 'address', 'fromBlock', and 'toBlock'.
        w3 (Web3): An instance of a Web3 connection to an Ethereum node.

    Returns:
        list: A list of event logs matching the query parameters.

    Raises:
        Exception: If the log fetching fails after the specified number of retries.
    """    
    
    # This loop implements retries for
    max_retries = 5
    for attempt in range(max_retries):
    
        try:
            logs = w3.eth.get_logs(parameters)
            return logs
        
        except requests.exceptions.ConnectionError as e:
            logger.error(f"ConnectionError encountered: {e}. Retrying...")
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTPError encountered: {e}. Retrying...")
        except requests.exceptions.RequestException as e:
            logger.error(f"Other RequestException encountered: {e}. Retrying...")
        time.sleep(DELAY)