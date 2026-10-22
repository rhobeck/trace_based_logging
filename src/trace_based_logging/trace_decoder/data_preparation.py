"""Decoding of the trace operations: the raw input of calls and delegated calls and
the topics and data of log entries are decoded with the ABI of the contract that was
called or that emitted the entry (Section 6 of the paper). ABIs come from Etherscan
and are cached across runs; log entries that the contract's ABI does not cover are
tried against the standard ERC event signatures and the custom events in
config_custom_events.json.

Functions: load_data, base_transformation, address_selection,
restrict_to_decoded_categories, convert_hex_to_int, create_abi_dict, decode_events,
decode_functions, process_abi, build_contract_objects.
"""
import numpy as np
import pandas as pd
import datetime
import re
import threading
import time
import requests
import json
from concurrent.futures import ThreadPoolExecutor
from web3 import Web3
import os
import pickle
from src.trace_based_logging.logging_config import setup_logging
from src.trace_based_logging import storage
from src.trace_based_logging.raw_trace_retriever.trace_retriever_utils import make_web3
from . import event_decoder



logger = setup_logging()

MAX_API_RETRIES = 5
ETHERSCAN_V2_URL = "https://api.etherscan.io/v2/api"
# conservative default; the limiter also reads the actual tier from Etherscan's
# rate-limit message ("... (3/sec)") and slows itself accordingly
ETHERSCAN_REQUESTS_PER_SECOND = 2.5
# rate-limit responses are pacing feedback, not errors: they get their own generous
# retry budget instead of consuming the MAX_API_RETRIES error budget
RATE_LIMIT_MAX_RETRIES = 30


class _RateLimiter:
    """Simple cross-thread minimum-interval limiter."""

    def __init__(self, requests_per_second):
        self._interval = 1.0 / requests_per_second
        self._lock = threading.Lock()
        self._next_at = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            delay = self._next_at - now
            self._next_at = max(now, self._next_at) + self._interval
        if delay > 0:
            time.sleep(delay)

    def slow_to(self, requests_per_second):
        """Never speed up; adopt a slower rate when the server names its limit."""
        with self._lock:
            self._interval = max(self._interval, 1.0 / requests_per_second)

# change the default look-up path to the directory above
dir_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..'))

def load_event_definitions(config_file):
    with open(config_file, 'r') as file:
        event_definitions = json.load(file)
    return event_definitions

def low(x):
    return x.lower()


def load_data(config, state, file_name_snippet):
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    base_contract = state["base_contract"]
    base = os.path.join(dir_path, "resources", config["log_folder"], "extraction",
                        f"{file_name_snippet}_{base_contract}_{config['min_block']}_{config['max_block']}")
    parquet_path, pkl_path, csv_path = base + ".parquet", base + ".pkl", base + ".csv"

    # Parquet is the canonical form; pickle and CSV are still read when present.
    if os.path.exists(parquet_path):
        data = pd.read_parquet(parquet_path)
        logger.info(f"Loaded {file_name_snippet} from parquet file.")
        # the DApp contract set is stored as a one-column table; return it as the set
        # the downstream code expects
        if list(data.columns) == ["address"]:
            return set(data["address"])
        return data
    elif os.path.exists(pkl_path):
        data = pd.read_pickle(pkl_path)
        logger.info(f"Loaded {file_name_snippet} from pickle file.")
        return data
    elif os.path.exists(csv_path):
        data = pd.read_csv(csv_path)
        logger.info(f"Loaded {file_name_snippet} from CSV file.")
        return data
    else:
        raise FileNotFoundError(f"No parquet, pickle or CSV file found for {base}.")


def base_transformation(df_log, contracts_dapp):
    """
    Performs the basic transformations of the trace operations before decoding:
    resets the index, converts 'timeStamp' from unix seconds to a UTC datetime,
    lowercases the 'to', 'from' and 'address' fields, renames 'type' and 'value' to
    'calltype' and 'callvalue', and flags the entries whose 'address' is a DApp
    contract.

    Args:
        df_log (pd.DataFrame): The trace operations; must have 'from', 'to',
                               'address' and 'timeStamp' columns.
        contracts_dapp (set or list): The addresses of the DApp contracts.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    logger.info("Start basic data transformation.")
    
    
    if not isinstance(df_log, pd.DataFrame):
        raise ValueError("The function input df_log must be a pandas DataFrame")

    if not isinstance(contracts_dapp, (list, set)):
        raise ValueError("The function input contracts_dapp must be a list or a set")

    required_columns = ['from', "to", 'address', 'timeStamp']
    missing_columns = [col for col in required_columns if col not in df_log.columns]
    if missing_columns:
        raise ValueError(f"The input DataFrame is missing required columns: {', '.join(missing_columns)}")


    df_log.reset_index(drop=True, inplace=True)

    try:
        # Block timestamps are unix seconds and are converted to UTC datetimes, so
        # absolute times do not depend on the machine the pipeline runs on.
        if "timeStamp" in df_log.columns:
            df_log["timeStamp"] = pd.to_datetime(pd.to_numeric(df_log["timeStamp"]), unit="s")
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


# config switches of the non-DApp categories that decoding uses ABIs for (creations need none)
NON_DAPP_DECODING_FLAGS = ("non_dapp_events", "non_dapp_calls", "non_dapp_zero_value_calls",
                           "non_dapp_delegatecalls")


def restrict_to_decoded_categories(addresses, contracts_dapp, config):
    """
    Keep only the addresses whose ABIs decoding will use. With every non-DApp category
    switched off, only DApp contracts are decoded (decoding.decode_all), so ABIs of
    other addresses would be fetched and never used; otherwise all addresses are kept.

    Args:
        addresses (list): Lower-cased addresses from address_selection.
        contracts_dapp (iterable): The DApp contract addresses.
        config (dict): The flat configuration with the non-DApp output switches.

    Returns:
        list: The addresses to look up ABIs for.
    """
    if any(config[flag] for flag in NON_DAPP_DECODING_FLAGS):
        return addresses
    dapp = set(map(low, contracts_dapp))
    selected = [a for a in addresses if a in dapp]
    logger.info(f"Non-DApp decoding switched off: looking up ABIs for {len(selected)} DApp "
                f"contracts, skipping {len(addresses) - len(selected)} other addresses.")
    return selected


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


def create_abi_dict(addresses, etherscan_api_key, abi_path=None, cache_path=None,
                    chainid=1, requests_per_second=ETHERSCAN_REQUESTS_PER_SECOND,
                    max_workers=4):
    """Retrieve contract ABIs from Etherscan (V2 getabi) for the given addresses.

    Returns a dict mapping lowercase address -> parsed ABI for every address with
    verified source code. "Contract source code not verified" is a data outcome and
    is recorded as such; API errors (rate limit, invalid key, transport) are retried
    and then raised, never recorded as an answer.

    Caching: results are persisted incrementally to cache_path (parquet) as they
    arrive — verified ABIs *and* not-verified markers — so interrupted runs resume
    and reruns skip everything already known. abi_path names a whole-dictionary
    pickle: if it exists, it is loaded and returned unchanged.
    """
    if abi_path and os.path.exists(abi_path):
        with open(abi_path, 'rb') as file:
            dict_abi = pickle.load(file)
        logger.info(f"ABI dictionary already exists and was loaded from: {abi_path}")
        return dict_abi

    addresses = [str(a).lower() for a in addresses]
    known = {}
    if cache_path and os.path.exists(cache_path):
        cached = pd.read_parquet(cache_path)
        known = {r.address: (bool(r.verified), r.abi) for r in cached.itertuples()}
        logger.info(f"ABI cache: {len(known)} known addresses loaded from {cache_path}")

    missing = [a for a in addresses if a not in known]
    logger.info(f"ABI retrieval: {len(addresses)} requested, "
                f"{len(addresses) - len(missing)} cached, {len(missing)} to fetch")

    if missing:
        limiter = _RateLimiter(requests_per_second)
        thread_local = threading.local()
        lock = threading.Lock()
        fetched = [0]

        def get_session():
            session = getattr(thread_local, "session", None)
            if session is None:
                session = requests.Session()
                thread_local.session = session
            return session

        def flush():
            if cache_path:
                df = pd.DataFrame(
                    [{"address": a, "verified": v, "abi": abi_text}
                     for a, (v, abi_text) in known.items()])
                storage.write_atomic_parquet(cache_path, df)

        def fetch(address):
            parameters = {"chainid": chainid, "module": "contract", "action": "getabi",
                          "address": address, "apikey": etherscan_api_key}
            attempt = 0
            rate_limit_hits = 0
            while attempt < MAX_API_RETRIES:
                limiter.wait()
                try:
                    payload = get_session().get(ETHERSCAN_V2_URL, params=parameters,
                                                timeout=30).json()
                except (requests.exceptions.RequestException, ValueError) as e:
                    attempt += 1
                    logger.error(f"ABI request error for {address} "
                                 f"(attempt {attempt}/{MAX_API_RETRIES}): {e}")
                    time.sleep(attempt)
                    continue
                status, result = payload.get("status"), payload.get("result")
                if status == "1":
                    return address, True, result            # verified: ABI JSON string
                if status == "0" and isinstance(result, str) \
                        and "not verified" in result.lower():
                    return address, False, ""               # data outcome, cached
                text = str(result)
                if "rate limit" in text.lower():
                    # pacing feedback: adapt to the tier the server names, retry
                    # without consuming the error budget
                    rate_limit_hits += 1
                    if rate_limit_hits > RATE_LIMIT_MAX_RETRIES:
                        raise RuntimeError(f"Persistent Etherscan rate limiting for "
                                           f"{address} ({rate_limit_hits} responses)")
                    tier = re.search(r"\((\d+(?:\.\d+)?)\s*/\s*sec\)", text)
                    if tier:
                        limiter.slow_to(float(tier.group(1)) * 0.8)
                    time.sleep(0.5 + 0.25 * rate_limit_hits)
                    continue
                attempt += 1
                logger.error(f"Etherscan non-success for {address}: "
                             f"{text[:100]!r} — retrying")
                time.sleep(attempt)
            raise RuntimeError(f"ABI retrieval failed for {address} after "
                               f"{MAX_API_RETRIES} attempts")

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for address, verified, abi_text in pool.map(fetch, missing):
                with lock:
                    known[address] = (verified, abi_text)
                    fetched[0] += 1
                    if fetched[0] % 200 == 0:
                        flush()
                        n_verified = sum(1 for v, _ in known.values() if v)
                        logger.info(f"ABI retrieval: {fetched[0]}/{len(missing)} fetched "
                                    f"({n_verified} verified so far)")
        flush()

    dict_abi = {a: json.loads(known[a][1]) for a in addresses if known[a][0]}
    logger.info(f"{len(dict_abi)} contract ABI(s) available; "
                f"{len(addresses) - len(dict_abi)} without verified ABI")
    return dict_abi

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

    # A parquet round-trip turns the topics lists into numpy arrays, whose truth
    # value is ambiguous inside the vendored decoder — normalize back to lists.
    df_events_raw["topics"] = df_events_raw["topics"].map(
        lambda t: list(t) if t is not None and not isinstance(t, list) and hasattr(t, "__iter__") else t)

    # free up memory
    del df_log

    unknown_event_count = 0
    
    max_row = len(df_events_raw)
    progress_step = max(1, max_row // 10)

    unknown_event_addresses = set()
    txs_event_not_decoded = list()

    # load fallback_abis
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    path = os.path.join(dir_path, 'config_custom_events.json')
    fallback_abis = load_event_definitions(path)

    # Topic maps are computed once per contract ABI and once for the fallback ABIs;
    # recomputing them per row (keccak over every event signature) was the dominant
    # cost of this loop.
    topic_map_cache = event_decoder.build_topic_maps(dict_abi)
    fallback_topic_maps = event_decoder.build_fallback_topic_maps(fallback_abis)

    logger.info(f"Starting to decode events, number of entries: {len(df_events_raw)}")

    # Each event already has a row assigned in df_events_raw. The encoded data is accessed by looping through the dataframe. 
    accumulated_data = []
    for index, row in df_events_raw.iterrows():
        address = row["address"]
        topics = row["topics"]
        data = row["data"]
        try: 
            
            # Here the decoding happens
            decoded_event = event_decoder.event_decoder(
                address, topics, data, dict_abi, fallback_abis,
                topic_map_cache=topic_map_cache, fallback_topic_maps=fallback_topic_maps)
            
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
                # bare ints as exact decimal strings (see decode_functions): avoids
                # float64 column coercion and the resulting precision loss
                if isinstance(value, int) and not isinstance(value, bool):
                    value = str(value)
                colNames.append(colName)
                values.append(value)

            # Existing data in the dataframe df_events_raw (e.g., on the trace) is reused in the new dataframe
            trace_data_values = [
                row["timeStamp"], row["tracePos"], row["tracePosDepth"],
                row["hash"], row["blockNumber"], row["transactionIndex"],
                row.get("flag_reverted")
                ]
            trace_data_colNames = ["timeStamp", "tracePos", "tracePosDepth", "hash", "blockNumber", "transactionIndex", "flag_reverted"]

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

        except Exception:
            # event not decodable with any available ABI: keep the raw row
            unknown_event_addresses.add(row["address"])
            unknown_event_count += 1
            logger.debug(f"Event not decodable with available ABIs: {row['hash']}")
            txs_event_not_decoded.append(row["hash"])
            columns = list(df_events_raw.columns.values)
            row_data = dict(zip(columns, list(row)))
            accumulated_data.append(row_data)

        if (index + 1) % progress_step == 0:
            logger.info(f"Events decoding: {index + 1} out of {max_row} events decoded.")
            
    logger.info(f"Events decoding: DONE. {unknown_event_count} unknown events occurred. Now building dataframe.")
    
    # free up memory
    del df_events_raw
    
    df_events = pd.DataFrame(accumulated_data)
    if "address" in df_events.columns:
        df_events["address"] = df_events["address"].str.lower()

      
    return df_events#, txs_event_not_decoded, unknown_event_addresses


# TODO: decoding ABIs could be decoded once in one go, and the function could be isolated 

def zero_value_mask(callvalue):
    """The zero-value test of the category split, in one module-owned place
    (tolerates both the raw '0x0' form and the int form after convert_hex_to_int)."""
    return callvalue.apply(lambda x: x == 0.0 or x == "0x0")


def decode_functions(df_log, dict_abi, node_url, calltype_list, include_zero_value_transactions, logging_string, contract_objects=None):
    """
    Decodes function call data from Ethereum transaction logs using the contract ABIs. It filters transactions based
    on specified call types and non-zero Ether transfer values, then attempts to decode each transaction's input data.

    Args:
        df_log (pd.DataFrame): DataFrame containing Ethereum transaction logs with columns for transaction data such as
                               'calltype', 'callvalue', 'input', etc.
        dict_abi (dict): A dictionary mapping contract addresses (as lowercase hex strings) to their respective ABIs.
        node_url (str): The URL of the Ethereum node used to connect via Web3.
        calltype_list (list): A list of call types (e.g., ['CALL', 'DELEGATECALL']) to filter the transactions by.
        include_zero_value_transactions (bool): True selects calls without Ether transfer, False calls with Ether
                               transfer, None every call (used for DELEGATECALLs, which have no Ether transfer of their own).

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
    mask_zero = zero_value_mask(df_log["callvalue"])
    if include_zero_value_transactions is None:
        # DELEGATECALLs are not annotated with ETH transfers, they propagate the value from the parent scope (Section 7).
        # The category holds every DELEGATECALL, whatever value the client reports for the frame.
        mask_callvalue = pd.Series(True, index=df_log.index)
    elif include_zero_value_transactions:
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
    
    # Contract objects are built once per run and shared across the eight decode
    # passes (see build_contract_objects); building here remains for direct calls.
    if contract_objects is None:
        contract_objects, failed = build_contract_objects(dict_abi, node_url)
        addresses_noAbi.update(failed)

    logger.info(f"Function decoding starting for {str(calltype_list)}, number of entries: {len(df_function_raw)}")
    
    max_row = len(df_function_raw)
    progress_step = max(1, max_row // 10)
    start = time.time()
    accumulated_data = []


    for index, row in df_function_raw.iterrows():
        
        
        if (index + 1) % progress_step == 0:
            logger.info(f"Function decoding: {logging_string} {str(calltype_list)}, {index + 1} out of {max_row} function calls decoded. Decoding failed for {unknown_functions_count}")

        
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
                # bare ints as exact decimal strings: a single missing value in a
                # chunk otherwise coerces the whole column to float64 (silent
                # precision loss above 2**53 wei)
                if isinstance(value, int) and not isinstance(value, bool):
                    value = str(value)
                colNames.append(colName)
                values.append(value)

            # use data from existing DataFrame
            trace_data_values = [row["from"],  row["to"], 
                                    row["gas"], row["gasUsed"],
                                    row["output"], row["callvalue"],
                                    row["calltype"], row["hash"],
                                    row["timeStamp"], row["tracePos"],
                                    row["tracePosDepth"], row["blockNumber"],
                                    row["transactionIndex"],
                                    row.get("flag_reverted")
                                    ]
            trace_data_colNames = ["from", "to", "gas", "gasUsed", "output", "callvalue", "calltype", "hash", "timeStamp", "tracePos", "tracePosDepth", "blockNumber", "transactionIndex", "flag_reverted"]

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
        # Unbounded integer columns are emitted as exact decimal strings BEFORE
        # frame construction: a single missing value would otherwise coerce the
        # whole column to float64 (silent precision loss above 2**53 wei), and a
        # single value above 2**63 would make the persisted dtype flip per chunk.
        # The transformation's int(x, 0) parses decimal strings losslessly.
        for row_data in segment_data:
            for col in ("gas", "gasUsed", "callvalue"):
                v = row_data.get(col)
                if v is not None and not isinstance(v, str) and pd.notna(v):
                    row_data[col] = str(int(v))
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

def process_abi(abi, contract_address_tmp, w3_or_node_url):
    """Initialize a web3 contract object for decoding function inputs.

    Accepts either a connected Web3 instance or a node URL; passing a shared instance
    avoids reconnecting per contract. ABIs are passed to web3 as parsed JSON.
    """
    w3 = w3_or_node_url if hasattr(w3_or_node_url, "eth") else make_web3(w3_or_node_url)
    if isinstance(abi, str):
        abi = json.loads(abi)
    address_checksum = Web3.to_checksum_address(contract_address_tmp)
    return w3.eth.contract(address=address_checksum, abi=abi)


def build_contract_objects(dict_abi, node_url):
    """Build the web3 contract objects for all ABIs once, shared across the decode
    passes (they were previously rebuilt for each of the eight passes).

    Returns (contract_objects, failed_addresses).
    """
    w3 = make_web3(node_url)
    contract_objects = {}
    failed = set()
    for address, abi in dict_abi.items():
        try:
            contract_objects[address.lower()] = process_abi(abi, address, w3)
        except Exception as e:
            logger.error(f"Failed to process ABI for address {address}: {e}")
            failed.add(address.lower())
    return contract_objects, failed
