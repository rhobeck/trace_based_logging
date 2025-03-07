import os
import pandas as pd
import src.trace_based_logging.log_construction.log_construction_utils as log_construction_utils
from web3 import Web3


def define_address_columns():
    columns_events = [
    "from", # EOAs, DApp contracts, Non-DApp contracts
    'to', # EOAs, DApp contracts, Non-DApp contracts
    'address', # DApp contracts
    '_from', # EOAs, DApp contracts, Non-DApp contracts
    '_to', # EOAs, DApp contracts, Non-DApp contracts
    'crowdsourcer', # DApp contracts (crowdsourcers)
    'market', # DApp contracts (markets)
    '_owner', # EOAs, DApp contracts, Non-DApp contracts
    '_address', # DApp contracts
    '_spender', # DApp contracts, Non-DApp contracts
    'target', # EOAs / users
    'token', # DApp contracts (Tokens)
    'feeWindow', # DApp contracts (fee windows)
    'marketCreator', # EOAs / users
    'creator', # EOAs / users
    'shareToken', # DApp contracts (SHARE tokens)
    'filler', # EOAs / users
    'sender', # EOAs / users
    'reporter', # EOAs / users
    'account', # EOAs / users
    'disputeCrowdsourcer', # DApp contracts (Dispute crowdsourcers)
    'contributor', # EOAs / users
    'contractAuthor', # EOAs as deployer
    'executor', # EOAs / users
    'owner', # EOAs and Non-DApp contracts
    'spender', # DApp contracts (REP tokens, inkl. REP v2)
    ]

    columns_calls_dapp = [
    'from', # EOAs, DApp contracts
    'to', # DApp contracts
    'address', # DApp contracts
    'denominationToken', # DApp contracts (Delegator)
    'designatedReporterAddress', # EOAs / users
    'sender', # EOAs / users
    'market', # DApp contracts (markets)
    ]

    columns_delegatecalls_dapp = [
    'from', # DApp contracts
    'to', # DApp contracts
    'spender', # DApp contracts, Non-Dapp contracts
    'denominationToken', # DApp contract (Delegator)
    'designatedReporterAddress', # EOAs / users
    'source', # EOAs / users, DApp contracts, Non-DApp contracts 
    'destination', # EOAs / users, DApp contracts, Non-DApp contracts
    'owner', # EOAs / users, DApp contracts, Non-DApp contracts
    'creator', # EOAs / users
    'feeWindow', # DApp contracts (fee windows)
    'market', # DApp contracts (markets)
    'designatedReporter', # EOAs / users
    'sender', # EOAs / users
    'reporter', # EOAs / users
    'target', # DApp contracts (Initial Reporter, Dispute Crowdsourcer)
    'buyer', # EOAs / users
    'participant', # EOAs / users, DApp contracts (Disputer)
    'redeemer', # EOAs / users
    'newOwner', # EOAs / users
    ]

    columns_calls_zero_value_dapp = [
    'from', # EOAs / users, DApp contracts
    'to', # DApp contracts
    'controller', # DApp contracts, Non-Dapp contracts
    'market', # DApp contracts (Markets)
    'owner', # EOAs / users, DApp contracts
    'target', # EOAs / users (there are a lot more unique addresses that the expected number of users)
    'from_function_internal', # EOAs / users, DApp contracts
    'to_function_internal', # EOAs / users, DApp contracts, Non-DApp contracts
    'feeWindow', # DApp contracts (Fee Window)
    'marketCreator', # EOAs / users
    'designatedReporter', # EOAs / users
    'creator', # EOAs / users
    'token', # DApp contract (Delegator)
    'shareToken', # DApp contracts (Share Token)
    'filler', # EOAs / users
    'sender', # EOAs / users
    'reporter', # EOAs / users
    'account', # EOAs / users
    'disputeCrowdsourcer', # DApp contracts (Dispute Crowdsourcer)
    'shareHolder', # EOAs / users
    'contributor', # EOAs / users
    'feeReceiver', # EOAs / users
    'newOwner', # EOAs / users
    'spender', # DApp contracts (REP tokens, inkl. REP v2)
    ]

    columns_creations_dapp = ["from", "to"]

    return  columns_events, columns_calls_dapp, columns_delegatecalls_dapp, columns_calls_zero_value_dapp, columns_creations_dapp


# identify addresses in the (dapp) log
def define_addresses(columns, df):
    addresses = set()
    for column in columns:
        addresses.update(df[column].dropna())
    addresses = {str(address).lower() for address in addresses}    
    return addresses


def get_min_block_numbers(addresses_df, address_cols, addresses):
    """
    Compute the earliest block number for each address.
    Returns:
    - dict: A dictionary with addresses as keys and their earliest blockNumber as values.
    """
    # Ensure all strings in the address_cols are lowercase and handle non-string entries
    for col in address_cols:
        if addresses_df[col].dtype != "object":  # Convert non-strings to strings
            addresses_df[col] = addresses_df[col].astype(str)
        addresses_df[col] = addresses_df[col].str.lower().fillna("")  # Convert to lowercase and handle NaN

    # **Add this step to normalize the 'addresses' set**
    # Ensure all addresses in the 'addresses' set are lowercase
    addresses = {str(address).lower() for address in addresses}

    # Reshape the DataFrame to have a single 'melted_address' column to avoid conflicts
    melted_df = addresses_df.melt(
        id_vars='blockNumber',
        value_vars=address_cols,
        value_name='melted_address'  # Changed from 'address' to 'melted_address'
    )

    # Ensure the 'melted_address' column is cleaned similarly
    melted_df['melted_address'] = melted_df['melted_address'].str.lower().fillna("")

    # Filter for the addresses of interest
    filtered_df = melted_df[
        melted_df['melted_address'].isin(addresses) & (melted_df['melted_address'] != "")
    ]

    # Group by 'melted_address' and find the minimal blockNumber
    min_block_numbers_df = filtered_df.groupby('melted_address', as_index=False)['blockNumber'].max()

    # Convert the DataFrame to a dictionary
    min_block_numbers_dict = dict(
        zip(min_block_numbers_df['melted_address'], min_block_numbers_df['blockNumber'])
    )

    return min_block_numbers_dict


def combine_min_block_numbers(dict_list):
    """
    Combine multiple dictionaries into one, keeping only the minimum blockNumber for each address.
    
    Parameters:
    - dict_list: List[dict] - A list of dictionaries with addresses as keys and blockNumbers as values.
    
    Returns:
    - dict: A single dictionary with each address and its minimum blockNumber.
    """
    combined_dict = {}
    for d in dict_list:
        for address, block_number in d.items():
            if address in combined_dict:
                combined_dict[address] = min(combined_dict[address], block_number)
            else:
                combined_dict[address] = block_number
    return combined_dict


def annotate_addresses(addresses, addresses_w_min_block_numbers, node_url, creations, contracts_dapp, mappings):
    w3 = Web3(Web3.HTTPProvider(node_url))
    contract_name_map = log_construction_utils.label_contracts_by_relative(creations, contracts_dapp, mappings["factory_contract_map"])
    address_dict = {}
    for address in addresses:
             
        dapp_flag = dapp_check(address, contracts_dapp)
        
        address_type = address_type_check(address, addresses_w_min_block_numbers, w3)

        contract_label = log_construction_utils.label_contract(address, mappings, contract_name_map)
            
        address_dict[address] = {"dapp_flag": dapp_flag, "type": address_type, "contract_label": contract_label}

    return address_dict


def address_type_check(address, addresses_w_min_block_numbers, w3):
    # TODO: Caveat: self-destructed contracts also have "0x" as code, see: 0xE9F42B1013F90Bb16eaB5382936cC7F9804dcFc5
    address_checksum = Web3.toChecksumAddress(address)
    block_number = addresses_w_min_block_numbers[address]
    byte_res = w3.eth.getCode(address_checksum, block_identifier=block_number)
    
    if byte_res.hex() == "0x":
        address_type = "EOA"
    else:
        address_type = "CA"

    return address_type


def dapp_check(address, contracts_dapp):
    # Check if contract is part of DApp
    if address in contracts_dapp:
        dapp_flag = "dapp"
    else: 
        dapp_flag = "non_dapp"
    return dapp_flag



def create_address_dict(base_contract, log_folder, dir_path, min_block, max_block, contracts_dapp, node_url, creations, mappings):
    
    columns_events, columns_calls_dapp, columns_delegatecalls_dapp, columns_calls_zero_value_dapp, columns_creations_dapp = define_address_columns()
    
    path = os.path.join(dir_path, "resources", log_folder, 'calls_dapp_zero_value_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.csv')
    cols = columns_calls_zero_value_dapp + ["blockNumber"]
    calls_dapp_zero_value = pd.read_csv(path, usecols=cols)
    #calls_dapp_zero_value = pickle.load(open(path, "rb"))
    addresses_calls_zero_dapp = define_addresses(columns_calls_zero_value_dapp, calls_dapp_zero_value)
    dict_calls_zero_dapp = get_min_block_numbers(calls_dapp_zero_value, columns_calls_zero_value_dapp, addresses_calls_zero_dapp)
    del calls_dapp_zero_value


    path = os.path.join(dir_path, "resources", log_folder, 'events_dapp_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.csv')
    cols = columns_events + ["blockNumber"]
    events_dapp = pd.read_csv(path, usecols=cols)
    #events_dapp = pickle.load(open(path, "rb"))
    addresses_events_dapp = define_addresses(columns_events, events_dapp)
    dict_events_dapp = get_min_block_numbers(events_dapp, columns_events, addresses_events_dapp)
    del events_dapp

    path = os.path.join(dir_path, "resources", log_folder, 'calls_dapp_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.csv')
    cols = columns_calls_dapp + ["blockNumber"]
    calls_dapp = pd.read_csv(path, usecols=cols)
    #calls_dapp = pickle.load(open(path, "rb"))
    addresses_calls_dapp = define_addresses(columns_calls_dapp, calls_dapp)
    dict_calls_dapp = get_min_block_numbers(calls_dapp, columns_calls_dapp, addresses_calls_dapp)
    del calls_dapp

    path = os.path.join(dir_path, "resources", log_folder, 'delegatecalls_dapp_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.csv')
    cols = columns_delegatecalls_dapp + ["blockNumber"]
    delegatecalls_dapp = pd.read_csv(path, usecols=cols)
    #delegatecalls_dapp = pickle.load(open(path, "rb"))
    addresses_delegatecalls_dapp = define_addresses(columns_delegatecalls_dapp, delegatecalls_dapp)
    dict_delegatecalls_dapp = get_min_block_numbers(delegatecalls_dapp, columns_delegatecalls_dapp, addresses_delegatecalls_dapp)
    del delegatecalls_dapp

    path = os.path.join(dir_path, "resources", log_folder, 'creations_dapp_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.csv')
    cols = columns_creations_dapp + ["blockNumber"]
    creations_dapp = pd.read_csv(path, usecols=cols)
    #delegatecalls_dapp = pickle.load(open(path, "rb"))
    addresses_creations_dapp = define_addresses(columns_creations_dapp, creations_dapp)
    dict_creations_dapp = get_min_block_numbers(creations_dapp, columns_creations_dapp, addresses_creations_dapp)
    del creations_dapp

    addresses = contracts_dapp | addresses_events_dapp | addresses_calls_dapp | addresses_calls_zero_dapp | addresses_delegatecalls_dapp

    dict_list = [dict_calls_zero_dapp, dict_events_dapp, dict_calls_dapp, dict_delegatecalls_dapp, dict_creations_dapp]

    addresses_w_min_block_numbers = combine_min_block_numbers(dict_list)

    address_dict = annotate_addresses(addresses, addresses_w_min_block_numbers, node_url, creations, contracts_dapp, mappings)
    
    return address_dict