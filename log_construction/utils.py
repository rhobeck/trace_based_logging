import pandas as pd
import json

def low(x):
    return x.lower()


def initial_transformation_events(df, dapp_flag, txs_reverted):
    # documentation written by ChatGPT
    """
    Performs initial transformations on the given DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame containing event data.
        dapp_flag (bool): Flag indicating whether to filter DApp contracts.

    Returns:
        pd.DataFrame: Transformed DataFrame.

    Raises:
        None

    Example:
        df_transformed = initial_transformation_events(df, True)
    """
    #df.dropna(subset=["name"], inplace=True)
    df.loc[df['name'].isnull(),'name'] = "undecoded event"
    # Convert addresses to lowercase
    df["address_lower"] = df[~df["address"].isnull()]["address"].apply(lambda x: x.lower())
    if dapp_flag == True: 
        df["market"] = df[~df["market"].isnull()]["market"].apply(lambda x: x.lower())

    # Rename columns for consistency
    df.rename(columns={"hash": "txHash"}, inplace=True)
    df.rename(columns={"name": "Activity"}, inplace=True)

    # Remove unnecessary columns if present
    if "Unnamed: 0" in df.columns.values.tolist():
        df.drop(["Unnamed: 0"], inplace=True, axis=1)
    if "functionName" in df.columns.values.tolist():
        df.drop(["functionName"], inplace=True, axis=1)

    # Add a flag for all log entries
    df["flag_log_entry"] = True
    
    # flag reverted oerations
    mask_reverted = df["txHash"].isin(list(txs_reverted))
    df["flag_reverted"] = mask_reverted
    
    # Add a flag for DApp contracts based on the provided flag
    if dapp_flag:
        df["flag_dapp_contract"] = True
    else:
        df["flag_dapp_contract"] = False

    return df


def rename_events(df_log, event_map_invert):
    # documentation written by ChatGPT
    """
    Rename events in a DataFrame using a given mapping.

    Args:
        df_log (pd.DataFrame): The input DataFrame containing an "Activity" column to be renamed.
        event_map_invert (dict): A dictionary mapping new event names to original event names.

    Returns:
        pd.DataFrame: A new DataFrame with the "Activity" column renamed according to the provided mapping.

    Example:
        >>> new_df = rename_events(df, {"new_event": "old_event"})
    """
    # Create a dictionary to map event names
    event_map = {v: k for k, v in event_map_invert.items()}

    # Define a mapping function
    def map_activity_names(x):
        return event_map.get(x, x)  # Use get to handle cases where x is not in the event_map

    # Create a new DataFrame with the modified "Activity" column
    try: 
        df_log_new = df_log.copy()
    except AttributeError:
        print("An AttributeError occurred. Make sure the dataframe representing the log is defined.")

    df_log_new["Activity"] = df_log["Activity"].apply(map_activity_names)

    return df_log_new


def rename_attribute(df_log, attribute, attribute_new, attribute_map):
    # documented with ChatGPT
    """
    Renames values in a specified column of a DataFrame based on a given mapping.

    Parameters:
    df_log (pandas.DataFrame): The DataFrame to be modified.
    attribute (str): The name of the column in the DataFrame whose values are to be renamed.
    attribute_map (dict): A dictionary mapping existing column values to new values.

    Returns:
    pandas.DataFrame: A DataFrame with renamed values in the specified column.

    Raises:
    KeyError: If the specified attribute is not a column in the DataFrame.
    """
    # Check if the attribute exists in the DataFrame
    if attribute not in df_log.columns:
        raise KeyError(f"Attribute '{attribute}' not found in DataFrame.")

    # Rename the values using the mapping. Unmapped values remain unchanged.
    df_log[attribute_new] = df_log[attribute].astype(str).map(attribute_map).fillna(df_log[attribute])

    return df_log


def combine_attributes(df, attribute_1, attribute_2, attribute_target, connector_string, attribute_exclusion_parameters):
    """
    Combines two attributes (columns) of a DataFrame into a target attribute.

    This function concatenates the string representations of two specified columns 
    (attributes) in a DataFrame, separated by a space, and stores the result in a 
    new target column. The operation is performed only for rows where the first 
    attribute is not null.

    Args:
    df (pandas.DataFrame): The DataFrame to be modified.
    attribute_1 (str): The name of the first attribute (column) to combine.
    attribute_2 (str): The name of the second attribute (column) to combine.
    attribute_target (str): The name of the target attribute (column) where the 
        combined result will be stored.

    Returns:
    pandas.DataFrame: The DataFrame with the combined attributes.

    Raises:
    KeyError: If any of the specified attributes are not in the DataFrame.
    TypeError: If the input df is not a pandas DataFrame.
    """
    # Check if the input is a pandas DataFrame
    if not isinstance(df, pd.DataFrame):
        raise TypeError("The input 'df' must be a pandas DataFrame.")

    # Check if the attributes exist in the DataFrame
    for attribute in [attribute_1, attribute_2]:
        if attribute not in df.columns:
            raise KeyError(f"Attribute '{attribute}' not found in DataFrame.")

    # Combine the attributes
    mask_exclusion_attribute_1 = ~df[attribute_1].isin(attribute_exclusion_parameters)
    mask_attribute_2 = ~df[attribute_2].isnull()
    mask = mask_exclusion_attribute_1 & mask_attribute_2
    df.loc[mask, attribute_target] = df.loc[mask, attribute_1].astype(str) + connector_string + df.loc[mask, attribute_2].astype(str)

    return df


def count_events(df_log, col):
    # documentation written by ChatGPT
    """
    Counts and prints the number of occurrences for each unique activity in the given DataFrame.

    Args:
        df_log (pd.DataFrame): Input DataFrame containing event log data.

    Returns:
        None

    Raises:
        None

    Prints:
        Displays the count of occurrences for each unique activity in the DataFrame.

    Example:
        count_events(df_log)
    """
    i_max = len(df_log[col].unique())

    for i in range(0, i_max):
        activity = df_log[col].unique()[i]
        print(len(df_log[df_log[col] == activity]), activity)


def create_contract_sensitive_events(df_log, mappings, creations, contracts_dapp, activity_split_candidates):
    # documentation in part with ChatGPT
    """
    Processes a log dataframe to create contract sensitive events. It includes renaming events,
    identifying factory contracts, and labeling contracts with specific names.

    Args:
        df_log (DataFrame): The dataframe containing log data.
        mappings (dict): A dictionary containing various mappings needed for processing.
        specific_event_mapping (str): Key to access the specific event mapping in `mappings`.
        creations (DataFrame): A dataframe containing information about contract creations.
        contracts_dapp (list): A list of contracts related to the dApp.
        activity_split_candidates (list): A list of activities to be considered for renaming.

    Returns:
        DataFrame: The processed dataframe with contract sensitive event names.

    The function executes the following steps:
    1. Renames events in `df_log` based on `specific_event_mapping`.
    2. Identifies factory contracts and labels them. This process is currently manual and can be
       automated in the future.
    3. Labels contracts by their factory relation if applicable.
    4. Assigns contract names to the log dataframe and handles known specific contracts.
    5. Appends suffixes to event names for contracts with inheritance sensitivity.
    6. Where contracts are not identified, retains the original activity names.
    """

    # create a dataframe with contracts that created other contracts. Those should be factories. Labeling them is a manual task (for now). Could be done with source code and LLM.
    # df_factories = utils.identify_factories(creations, contracts_dapp)

    # now try to identify what the factory contracts are doing and label them
    # the following approach only works for one tier of factories
    contract_name_map = label_contracts_by_relative(creations, contracts_dapp, mappings["factory_contract_map"])

    # assign the contract names 
    df_log['contract_name'] = df_log['address_lower'].map(contract_name_map)
    df_log['factory_name'] = df_log['address_lower'].map(mappings["factory_contract_map"])
    # read map_specific_known_contracts to not include the labels in the activity name (e.g., delegators, main contract, REP Token)
    df_log['contract_name_specifics'] = df_log['address_lower'].map(mappings["map_specific_known_contracts"])
    # manually tested root deployments (and others) for known contracts
    # events_dapp[events_dapp["contract_name"] == "RootDeployer_1Creation"]["address"].unique()

    # replace inheritance for known factory contracts
    mask_factory_name = df_log['factory_name'].notnull()
    df_log.loc[mask_factory_name, "contract_name"] = df_log.loc[mask_factory_name, "factory_name"]
    df_log.drop(["factory_name"], inplace=True, axis=1)
    
    # replace inheritance for known factory contracts
    mask_specific_name = df_log['contract_name_specifics'].notnull()
    df_log.loc[mask_specific_name, "contract_name"] = df_log.loc[mask_specific_name, "contract_name_specifics"]
    df_log.drop(["contract_name_specifics"], inplace=True, axis=1)

    list_all_activities = df_log["Activity"].unique()
    excluded_events = list(set(list_all_activities) - set(activity_split_candidates))

    df_log = combine_attributes(df_log, "Activity", "contract_name", "Activity_contract_sensitive", ", contract: ", excluded_events)

    return df_log


def fix_json_string(s):
    """Attempt to fix JSON string by appending a double quote and a an curvy bracket "}" at the end."""
    return s + '"}'


def safe_load_json(s):
    """Load json file with potential error handling, if the extraInfo is truncated"""
    try:
        return json.loads(s)
    except json.JSONDecodeError as e:
        if 'Unterminated string starting at' in str(e):
            fixed_s = fix_json_string(s)
            return json.loads(fixed_s)
        else:
            # Re-raise the exception if it's not the specific 'unterminated string' error.
            raise


def propagate_extraInfo(df_events_dapp):
    # documentation written by ChatGPT
    """
    Propagate extraInfo information from 'create market' entries to related activities.

    Args:
        df_events_dapp (pd.DataFrame): Input DataFrame containing 'Activity', 'marketId', and 'extraInfo' columns.

    Returns:
        pd.DataFrame: DataFrame with extraInfo information propagated to related activities.

    """
    # Step 1: Identify the 'marketId' associated with each 'create market' entry
    market_ids = df_events_dapp.loc[df_events_dapp['Activity'] == 'create market', 'market']
    market_ids.reset_index(drop=True, inplace=True)

    # Step 2: Extract and parse the 'extraInfo' for 'create market' entries
    market_info = df_events_dapp.loc[df_events_dapp['Activity'] == 'create market', 'extraInfo'].dropna().apply(safe_load_json)
    market_info = pd.json_normalize(market_info)
    market_info["market"] = market_ids
    
    return market_info

    
def propagate_marketType(df_events_dapp):
    # written by ChatGPT
    """
    Propagate marketType information from 'create market' entries to related activities.

    Args:
        df_events_dapp (pd.DataFrame): Input DataFrame containing 'Activity', 'marketId', and 'marketType' columns.

    Returns:
        pd.DataFrame: DataFrame with marketType information propagated to related activities.

    """
    # Identify the 'marketId' associated with each 'create market' entry
    market_ids = df_events_dapp.loc[df_events_dapp['Activity'] == 'create market', 'market']
    market_ids.reset_index(drop=True, inplace=True)

    # Extract the 'marketType' for 'create market' entries
    market_type_info = df_events_dapp.loc[df_events_dapp['Activity'] == 'create market', ['market', 'marketType']].dropna()
    
    # Adjust names of the markets
    market_type_info["marketType"].loc[market_type_info["marketType"] == 0] = "yes-no"
    market_type_info["marketType"].loc[market_type_info["marketType"] == 1] = "categorical"
    market_type_info["marketType"].loc[market_type_info["marketType"] == 2] = "scalar"
    
    return market_type_info


def identify_factories(creations, contracts_dapp):
    mask_dapp_contracts = creations["from"].isin(contracts_dapp)
    #mask_create2 = df_trace_l1["type"]=="DELEGATECALL"
    df_create = creations[mask_dapp_contracts] 

    issued = df_create["from"].tolist()
    received = df_create["to"].tolist()

    list_appearance = []
    for contract in contracts_dapp:
        list_appearance.append([contract, issued.count(contract)])

    df_appearance = pd.DataFrame(list_appearance, columns=["Address", "contractsCreated"])

    #sum issued
    print("Number of creation calls issued by DApp contracts: ", sum(df_appearance["contractsCreated"]))
    #print("Number of calls received by DApp contracts: ", sum(df_appearance["received"]))

    df_intern = df_create[df_create['from'].isin(contracts_dapp)]
    df_intern = df_intern[df_intern['to'].isin(contracts_dapp)]

   # more calls received than issued
    mask_count = df_appearance["contractsCreated"] != 0
    head_count_non_zero = sum(mask_count)
    df_factories = df_appearance.sort_values("contractsCreated", ascending=False).head(head_count_non_zero)
    return df_factories


def label_contracts_by_relative(creations, contracts_dapp, factory_contract_map):
    mask_dapp_contracts = creations["from"].isin(contracts_dapp)
    df_create = creations[mask_dapp_contracts]
    # Create an empty dictionary to store the relationships
    contract_relationships = {}

    # Iterate through each row in the DataFrame
    for _, row in df_create.iterrows():
        creator = row['from']
        child = row['to']

        # Check if the creator is already a key in the dictionary
        if creator in contract_relationships:
            # If yes, append the child to the list of children
            contract_relationships[creator].append(child)
        else:
            # If no, create a new key with the creator and initialize a list with the child
            contract_relationships[creator] = [child]

    contract_name_map = {}
    for contract_factory in contract_relationships:
        if "Factory" in factory_contract_map[low(contract_factory)][-7:]:
            for contract in contract_relationships[low(contract_factory)]:
                contract_name_map[low(contract)] = factory_contract_map[low(contract_factory)][:-8]
        else:
            for contract in contract_relationships[low(contract_factory)]:
                contract_name_map[low(contract)] = factory_contract_map[low(contract_factory)] + " deployment"
    #    for contract in contract_relationships[contract_factory]:
    return contract_name_map


def find_between(s, first, last):
    # https://stackoverflow.com/questions/3368969/find-string-between-two-substrings
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""


def initial_transformation_calls(df_calls, dapp_flag, txs_reverted):

    df_calls["address_lower"] = df_calls[~df_calls["to"].isnull()]["to"].apply(lambda x: x.lower())
    
    if dapp_flag == True: 
        df_calls["market"] = df_calls[~df_calls["market"].isnull()]["market"].apply(lambda x: x.lower())

    df_calls.rename(columns={"hash": "txHash"}, inplace=True)

    # if transaction is reverted, flag
    mask_reverted = df_calls["txHash"].isin(list(txs_reverted))
    df_calls["flag_reverted"] = mask_reverted

    # Add a flag for DApp contracts based on the provided flag
    if dapp_flag:
        df_calls["flag_dapp_contract"] = True
    else:
        df_calls["flag_dapp_contract"] = False

    # Hex values to int
    val_list = ["gas", "gasUsed", "callvalue"]
    for val in val_list:
        df_calls.loc[:, val] = df_calls[val].apply(lambda x: int(x, 0) if str(x) != "nan" else None)
        
    # sort out Activity names
    # use only function name from the whole function call string
    df_calls["name"] = df_calls["name"].apply(lambda x: find_between(x, "<Function ", "(") if str(x) != "nan" else None)
    df_calls.loc[df_calls['calltype'].str.contains("CALL"),'Activity'] = df_calls['name']
    # for PM4Py actvities have to have a name. Find empty activity names and fill in type:
    df_calls.loc[df_calls['Activity'].isnull(),'Activity'] = df_calls['calltype']

    # give all calls the flag: message_call
    
    df_calls["flag"] = "message_call"
    
    return df_calls


def get_events(df, act, address_attribute):
    mask = df["Activity_contract_sensitive"] == act
    df = df[mask]
    df.reset_index(inplace = True)
    address = df[address_attribute]
    return address

'''
act_list = events_dapp["Activity_contract_sensitive"].unique()
list_act = []
for act in act_list:
    list_contracts = get_events(events_dapp, act, "to").unique()
    if len(list_contracts) != 1:
        list_act.append(act)
    print(list_act)
'''