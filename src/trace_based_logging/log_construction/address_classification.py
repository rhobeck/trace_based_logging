"""
Classification of addresses into externally owned accounts (EOA) and contract accounts (CA).

The account type is the object attribute of Table 3 in the paper. An address is classified as CA if
    1. it was created in the transaction traces (target of a CREATE or CREATE2; the zero address,
       which a failed creation reports, is not a created contract), or
    2. it holds bytecode at the maximum block of the extraction (eth_getCode), or
    3. it executed code in the transaction traces, i.e., it sent an internal (non-root) call
       frame or emitted a log entry. Only code can do either, so this also covers contracts
       that self-destructed before the maximum block.
All other addresses are classified as EOA.

This follows Section 7 of the paper: the initiator of a transaction is always an EOA (hence the
sender of an internal frame is always a CA), and the bytecode query identifies CAs except for
self-destructed contracts and failed creations, the two cases covered by rules 1 and 3.
Call and delegatecall targets are deliberately not used as evidence: the EVM executes a call to
an address without code as a successful no-op (Augur proxies delegatecall to the zero address
when a lookup is unset).

The previous version of this module queried the bytecode once, at the block in which an address
first appears. Evaluated on the full case study (154,519 addresses), that strategy agrees with
the rules above on 154,369 addresses and classifies the remaining 150 as EOA although they are
contracts: 121 contracts whose first creation attempt reverted (the retry deployed the same
address later), 20 contracts that appear as parameters before their deployment, and 8 contracts
that self-destructed within their first block. It adds no contract the rules above miss.

The classified addresses are exactly the addresses that become objects in the event log, read
the way the engine (ocel_construction) reads them: the address-typed object sources of the
mapping m and the role columns on every operation, the call frame fields 'from' and 'to' on
call and creation operations. A value there that is not an address is an error.
"""
import os
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests

from src.trace_based_logging import storage
from src.trace_based_logging.logging_config import setup_logging
from src.trace_based_logging.log_construction.ocel_construction import CALL_TYPES, CREATE_TYPES

logger = setup_logging()

ADDRESS_PATTERN = r"^0x[0-9a-f]{40}$"
ZERO_ADDRESS = "0x" + "0" * 40
FRAME_COLUMNS = ["from", "to"]


def define_address_columns(object_model, available_columns):
    """
    Define the columns that hold addresses which become objects in the event log.

    Args:
        object_model (dict): The mapping m (section 'object_model' of mappings.json).
        available_columns (iterable): The columns of the operations dataframe.

    Returns:
        tuple: The sorted frame columns ('from', 'to'; objects on call and creation
            operations only, as in the engine) and the sorted address columns of the
            mapping (objects on every operation), each restricted to available_columns.
    """
    mapping_columns = set(object_model.get("role_columns", {}).keys())
    for object_type in object_model["types"]:
        if object_type.get("address", False):
            for source in object_type["sources"]:
                mapping_columns.add(source["column"])
    frame_columns = set(FRAME_COLUMNS).difference(mapping_columns)
    return (sorted(frame_columns.intersection(available_columns)),
            sorted(mapping_columns.intersection(available_columns)))


def define_addresses(columns, df):
    """
    Identify the addresses in the given columns of a dataframe.

    Args:
        columns (list): The columns to read addresses from.
        df (pd.DataFrame): The dataframe.

    Returns:
        set: The lower-cased addresses (42-character hexadecimal strings).

    Raises:
        ValueError: If a column holds a value that is not an address (the engine would
            turn it into an object without account type).
    """
    addresses = set()
    for column in columns:
        if column in df.columns:
            values = df[column].dropna().astype(str).str.lower()
            malformed = values[~values.str.match(ADDRESS_PATTERN)]
            if not malformed.empty:
                raise ValueError(f"Column '{column}' holds {len(malformed):,} values that are "
                                 f"not addresses (e.g. {sorted(set(malformed))[:3]}).")
            addresses.update(values.unique())
    return addresses


def collect_addresses(operations, object_model):
    """
    Collect every address that becomes an object in the event log.

    Args:
        operations (pd.DataFrame): The operations (concatenated category tables).
        object_model (dict): The mapping m.

    Returns:
        set: The addresses to classify.
    """
    frame_columns, mapping_columns = define_address_columns(object_model, operations.columns)
    is_frame = operations["calltype"].isin(CALL_TYPES + CREATE_TYPES)
    addresses = define_addresses(frame_columns, operations[is_frame])
    # The null values of the mapping (e.g. the zero address as Augur's placeholder for
    # "no market") become no object in the engine, so they get no account type either:
    # the zero address is no account, neither EOA nor CA. Frame fields are not filtered,
    # as in the engine: a call to an address is a fact of the trace, not a parameter.
    null_values = {value.lower() for value in object_model.get("null_values", [])}
    addresses.update(define_addresses(mapping_columns, operations).difference(null_values))
    logger.info(f"Collected {len(addresses):,} addresses from the columns "
                f"{frame_columns + mapping_columns}.")
    return addresses


def created_addresses(trace_tree):
    """
    Identify the addresses created in the transaction traces (targets of CREATE / CREATE2),
    independent of which operation categories the event log includes. A creation that
    failed reports the zero address as its result and created nothing.

    Args:
        trace_tree (pd.DataFrame): The trace tree with the columns 'type' and 'to'.

    Returns:
        set: The created addresses.
    """
    creations = trace_tree[trace_tree["type"].isin(CREATE_TYPES)]
    created = define_addresses(["to"], creations)
    created.discard(ZERO_ADDRESS)
    return created


def contract_evidence(trace_tree):
    """
    Identify the addresses that executed code in the transaction traces: senders of internal
    (non-root) call frames and emitters of log entries.

    Args:
        trace_tree (pd.DataFrame): The trace tree with the columns 'type', 'tracePosDepth',
            'from', and 'address'.

    Returns:
        set: The addresses with evidence of being a contract.
    """
    is_internal_frame = (trace_tree["tracePosDepth"].astype(str) != "1") & trace_tree["type"].notna()
    senders = define_addresses(["from"], trace_tree[is_internal_frame])
    emitters = define_addresses(["address"], trace_tree)
    logger.info(f"Trace evidence: {len(senders):,} internal senders, {len(emitters):,} log emitters.")
    return senders.union(emitters)


def get_code_size(address, block_number, node_url, max_attempts=10, timeout=60):
    """
    Query the size of the bytecode of an address at a block (eth_getCode).

    Args:
        address (str): The address.
        block_number (int): The block at which the code is queried.
        node_url (str): The URL of the Ethereum node.
        max_attempts (int): Retries before giving up.
        timeout (int): Request timeout in seconds.

    Returns:
        int: The code size in bytes (0 for an address without code).
    """
    payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_getCode",
               "params": [address, hex(int(block_number))]}
    for attempt in range(max_attempts):
        try:
            response = requests.post(node_url, json=payload, timeout=timeout).json()
            if response.get("result") is not None:
                return (len(response["result"]) - 2) // 2
            logger.warning(f"eth_getCode for {address} returned {response.get('error')}.")
        except (requests.RequestException, ValueError) as error:
            logger.warning(f"eth_getCode for {address} failed: {error}.")
        time.sleep(min(30, attempt + 1))
    raise RuntimeError(f"eth_getCode failed for {address} at block {block_number}.")


def load_code_size_cache(cache_path, block_number):
    """
    Load previously queried code sizes, if they were queried at the same block.

    Args:
        cache_path (str): Path of the cache parquet file.
        block_number (int): The block the sizes must have been queried at.

    Returns:
        dict: Address -> code size.
    """
    if cache_path is None or not os.path.exists(cache_path):
        return {}
    cache = pd.read_parquet(cache_path)
    if not (cache["block"] == block_number).all():
        return {}
    return dict(zip(cache["address"], cache["code_bytes"]))


def save_code_size_cache(cache_path, code_sizes, block_number):
    """
    Save the queried code sizes.

    Args:
        cache_path (str): Path of the cache parquet file.
        code_sizes (dict): Address -> code size.
        block_number (int): The block the sizes were queried at.
    """
    if cache_path is None:
        return
    cache = pd.DataFrame({"address": list(code_sizes.keys()),
                          "code_bytes": list(code_sizes.values()),
                          "block": block_number})
    storage.write_atomic_parquet(cache_path, cache)


def fetch_code_sizes(addresses, get_code, block_number, cache_path=None, workers=3):
    """
    Query the code size of every address at a block, reusing cached results.

    Args:
        addresses (iterable): The addresses to query.
        get_code (callable): get_code(address, block_number) -> code size in bytes.
        block_number (int): The block at which the code is queried.
        cache_path (str): Path of the cache parquet file (None: no cache).
        workers (int): Number of concurrent queries.

    Returns:
        dict: Address -> code size, for every given address.
    """
    code_sizes = load_code_size_cache(cache_path, block_number)
    addresses_to_query = sorted(set(addresses).difference(code_sizes.keys()))
    logger.info(f"Querying the code of {len(addresses_to_query):,} addresses at block "
                f"{block_number} ({len(code_sizes):,} cached).")

    start_time = time.time()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = executor.map(lambda address: get_code(address, block_number), addresses_to_query)
        for count, (address, code_size) in enumerate(zip(addresses_to_query, results), start=1):
            code_sizes[address] = code_size
            if count % 5000 == 0:
                save_code_size_cache(cache_path, code_sizes, block_number)
                rate = count / (time.time() - start_time)
                logger.info(f"Queried {count:,} of {len(addresses_to_query):,} addresses ({rate:.0f}/s).")
    save_code_size_cache(cache_path, code_sizes, block_number)

    return {address: code_sizes[address] for address in addresses}


def classify_accounts(operations, object_model, trace_tree, max_block, get_code,
                      cache_path=None, known_contracts=()):
    """
    Classify every object address of the event log as EOA or CA.

    Args:
        operations (pd.DataFrame): The operations (concatenated category tables).
        object_model (dict): The mapping m.
        trace_tree (pd.DataFrame): The trace tree of the extraction with the columns 'type',
            'tracePosDepth', 'from', 'to', and 'address'.
        max_block (int): The maximum block of the extraction.
        get_code (callable): get_code(address, block_number) -> code size in bytes.
        cache_path (str): Path of the code size cache (None: no cache).
        known_contracts (iterable): Addresses that are contracts by configuration
            (the DApp's seed contracts).

    Returns:
        pd.DataFrame: One row per address with the columns 'address', 'account_type'
            ('EOA' / 'CA'), 'source' ('creations' / 'getCode' / 'trace_evidence'),
            and 'code_bytes'.
    """
    logger.info("Classifying addresses into EOAs and CAs.")
    addresses = collect_addresses(operations, object_model)

    created = created_addresses(trace_tree)
    created.update(address.lower() for address in known_contracts)
    created = created.intersection(addresses)
    logger.info(f"{len(created):,} addresses were created in the transaction traces.")

    evidence = contract_evidence(trace_tree).intersection(addresses)

    code_sizes = fetch_code_sizes(addresses.difference(created), get_code, max_block, cache_path)

    rows = []
    for address in sorted(addresses):
        if address in created:
            rows.append({"address": address, "account_type": "CA", "source": "creations",
                         "code_bytes": None})
        elif code_sizes[address] > 0:
            rows.append({"address": address, "account_type": "CA", "source": "getCode",
                         "code_bytes": code_sizes[address]})
        elif address in evidence:
            rows.append({"address": address, "account_type": "CA", "source": "trace_evidence",
                         "code_bytes": 0})
        else:
            rows.append({"address": address, "account_type": "EOA", "source": "getCode",
                         "code_bytes": 0})

    account_types = pd.DataFrame(rows, columns=["address", "account_type", "source", "code_bytes"])
    assert set(account_types["address"]) == addresses

    counts = account_types.groupby(["account_type", "source"]).size().to_dict()
    logger.info(f"Classified {len(account_types):,} addresses: {counts}.")
    return account_types
