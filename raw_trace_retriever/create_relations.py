from logging_config import setup_logging

logger = setup_logging()

def create_relations(df_trace_tree, contracts_dapp, set_contracts_lx, set_predefined_non_dapp_contracts):
    """
    Analyzes blockchain transaction traces stored in a dataframe to identify CREATE-relationships between contracts related to a DApp. 
    This process involves identifying contracts created by or that created known DApp contracts, and updating the set of contracts associated with the DApp. 
    Additionally, it filters out predefined non-DApp contracts.

    Args:
        df_trace_tree (pd.DataFrame): A DataFrame containing transaction trace data, which includes information on transactions with types "CREATE" or "CREATE2".
        contracts_dapp (set): A set of contract addresses known to be part of the DApp.
        set_contracts_lx (set): A dynamic set of contract addresses under investigation for their relevance to the DApp. It gets updated throughout the function execution.
        set_predefined_non_dapp_contracts (set): A set of contract addresses known not to be part of the DApp, used for filtering purposes.

    Returns:
        tuple: Returns a tuple containing two sets:
            - contracts_dapp (set): An updated set of contract addresses confirmed to be associated with the DApp after processing.
            - set_contracts_lx (set): A refined set of new contract addresses identified as related to the DApp, excluding those predefined as non-DApp contracts.

    Detailed Process:
        1. Filters the transaction traces to consider only "CREATE" and "CREATE2" transaction types as these indicate contract creation events.
        2. Cleans up the "from" addresses in the traces by removing any appended sequence numbers, retaining only the actual address part.
        3. Identifies relevant contracts based on their creation relationships. A contract is deemed relevant if:
            - It is created by a contract known to be part of the DApp.
            - It is a known DApp contract being created in the trace.
        4. Iteratively updates the set of DApp contracts as new relationships are discovered through the traces.
        5. Filters out any contracts identified as non-DApp contracts from the set of newly discovered contracts to ensure the purity of the DApp's contract ecosystem.
        
    Note:
        The function employs a temporary solution for identifying CREATE-relations by trimming the "from" addresses to their first 42 characters, due to a workaround for encoding the order of computational steps in the trace data. 
        Future improvements could refine how order and relationships are encoded and identified.
    """
    # Create masks to only consider CREATE traces to find contracts that were created by contracts we looked at
    # For now, all CREATE-relations are identified. We sort out relevant ones later.
    mask_create = df_trace_tree["type"]=="CREATE"
    mask_create2 = df_trace_tree["type"]=="CREATE2"
    mask_create = mask_create+mask_create2
    df_trace_tree_creates = df_trace_tree[mask_create]

    # Big issue in blockchain and therefore in the code is the ordering of events (essentially: http://www.workflowpatterns.com/patterns/logimperfection/elp1.php).
    # In order to insert an order, the position of a computational step in the trace tree was stored as an extension of the "from"-address in the JSON (attached _[count] to "from" address). This can be improved.
    # As a consequence, the "from"-address now has to be liberated from its coding accident related attachement. To fix the issue, only use the first 42 characters of the "from"-addresses (only characters that belong to the actual address), and omit the attached order.
    # This remains temporary for the identification of CREATE-relations

    # Written by ChatGPT
    # Check if 'from' is a string and has a length of at least 42 characters
    mask = (df_trace_tree_creates['from'].str.len() >= 42) & df_trace_tree_creates['from'].apply(lambda x: isinstance(x, str))
    # Use .loc to set values that meet the condition
    df_trace_tree_creates.loc[mask, 'from'] = df_trace_tree_creates['from'].str[:42]
        
    # Now identify relevant contracts in the CREATE-relations. Beware and exclude contracts that were created by accounts that do not belong to the DApp
    # There are two cases:
    # 1) Contracts were created by contracts we know belong to the DApp -> Created contracts are relevant (contracts that have "from"-address set_contracts_lx)
    # 2) Contracts we know belong to the DApp were created -> Creating contracts are relevant (contracts that have "to"-address set_contracts_lx)
    # Those contracts identified might then in turn have additional relevant CREATE-relations like 1) and 2), hence, we have move on iteratively and the the whole state of traces has to be considered

    # Make a copy of known DApp contracts to be able to compare against new contracts later
    contracts_dapp_minus_1 = contracts_dapp.copy()
    set_tmp = set()
    while set_tmp != set_contracts_lx: 
        set_tmp = set_contracts_lx.copy()
        creators_of_contracts = set(df_trace_tree_creates[df_trace_tree_creates['to'].isin(contracts_dapp)]["from"].tolist())
        # creations by contracts of the list (this has to happen after "creators_of_contracts" to not miss out on their creations)    
        creations_of_contracts = set(df_trace_tree_creates[df_trace_tree_creates['from'].isin(contracts_dapp)]["to"].tolist())
                
        creators_and_creations = creators_of_contracts.union(creations_of_contracts)
        contracts_dapp.update(creators_and_creations)
        set_contracts_lx.update(creators_and_creations)
        # print("NEW LIST CONTRACTS", set_contracts_lx)
        # print(set_tmp != set_contracts_lx)
    
       
    set_contracts_lx = contracts_dapp.difference(contracts_dapp_minus_1)
    # Remove contracts that are known for not belonging to the DApp from the list of newly discovered contracts.
    # Why? Because the next iterations looks for all CREATE-relations to and from the contracts is set_contracts_lx
    # If a deployment was not clean, (e.g., a root deployed more than one DApp), the branches to non-DApp CREATION-relations can be "pruned" by removing those contracts we know do not belong to the DApp anyways.
    # The alternative would be that contracts that do not belong to the DApp are identified as DApp contracts in the next iterations. We would end up with deployment trees of multiple DApps. 
    set_contracts_lx = remove_predefined_contracts(set_contracts_lx, set_predefined_non_dapp_contracts)

    """
    if "0x0000000000000000000000000000000000000000".lower() in set_contracts_lx:
        ts = datetime.datetime.fromtimestamp(time.time()).strftime('%d-%m-%Y %H:%M:%S')
        # print(ts, "0x0000000000000000000000000000000000000000 was excluded")
        set_contracts_lx.remove("0x0000000000000000000000000000000000000000")
    else: 
        ts = datetime.datetime.fromtimestamp(time.time()).strftime('%d-%m-%Y %H:%M:%S')
        # print(ts, "0x0000000000000000000000000000000000000000 was not in the contract list")
    """
    contracts_dapp = contracts_dapp.union(creators_and_creations)
    
    return contracts_dapp, set_contracts_lx


def remove_predefined_contracts(set_contracts_lx, set_predefined_non_dapp_contracts):
    """
    Removes all items in set_predefined_non_dapp_contracts from set_contracts_lx if any of 
    the predefined non-DApp contracts are found in the list of contracts.

    Args:
        set_contracts_lx (set of str): The set of contract addresses to be filtered.
        set_predefined_non_dapp_contracts (set of str): A set of predefined non-dApp 
            contract addresses that should be removed from set_contracts_lx if present.

    Returns:
        set of str: The filtered set of contract addresses with all predefined non-dApp 
            contracts removed if any were present.
    """
    # Making sure that two sets with unique values are compared
    set_contracts_lx = set(set_contracts_lx)
    set_predefined_non_dapp_contracts = set(set_predefined_non_dapp_contracts)

    # Check if any of the predefined non-dApp contracts are in the contracts list
    if set_contracts_lx.intersection(set_predefined_non_dapp_contracts):
        # Remove all predefined non-dApp contracts from the contracts list
        set_contracts_lx = set_contracts_lx - set_predefined_non_dapp_contracts
        
        # Log the removal
        logger.info(f"Predefined non-dApp contracts were excluded {set_predefined_non_dapp_contracts}")
    return set_contracts_lx
