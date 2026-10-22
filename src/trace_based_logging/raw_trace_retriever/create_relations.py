from src.trace_based_logging.logging_config import setup_logging

logger = setup_logging()


def create_relations(df_trace_tree, contracts_dapp, set_contracts_lx, contracts_non_dapp):
    """
    Identify CREATE-relationships between DApp contracts in transaction traces and grow
    the DApp contract set accordingly (the discovery step of Section 6).

    A contract is added to the DApp set when it is either created by a known DApp
    contract (forward search along the creation sub-tree) or is the creator of a known
    DApp contract (backward search). The search is iterated to a fixpoint over the
    current trace state; predefined non-DApp contracts are pruned so that unrelated
    creation branches of a shared deployer are not pulled in.

    Args:
        df_trace_tree (pd.DataFrame): trace operations, including CREATE / CREATE2 rows
            with 'from' (creator) and 'to' (created) address columns.
        contracts_dapp (set): addresses currently known to belong to the DApp.
        set_contracts_lx (set): addresses discovered in the previous level, whose
            CREATE-relations are to be explored now.
        contracts_non_dapp (set): addresses known not to belong to the DApp.

    Returns:
        tuple(set, set): the updated contracts_dapp set, and the set of newly
        discovered contracts to explore in the next level (non-DApp contracts pruned
        from both).
    """
    creates = df_trace_tree[df_trace_tree["type"].isin(["CREATE", "CREATE2"])]
    # Match on lowercased addresses; the DApp sets are lowercased at initialization and
    # callTracer emits lowercase addresses, but normalize here so matching cannot depend
    # on client casing.
    creators = creates["from"].astype(str).str.lower()
    created = creates["to"].astype(str).str.lower()

    contracts_dapp_before = contracts_dapp.copy()

    # Grow the DApp set to a fixpoint: contracts created by DApp contracts, and creators
    # of DApp contracts, are themselves DApp contracts.
    previous = None
    while previous != contracts_dapp:
        previous = contracts_dapp.copy()
        creators_of_dapp = set(creators[created.isin(contracts_dapp)])
        creations_of_dapp = set(created[creators.isin(contracts_dapp)])
        contracts_dapp = contracts_dapp | creators_of_dapp | creations_of_dapp

    # Newly discovered contracts become the frontier for the next extraction level,
    # minus any known non-DApp contracts (prunes foreign creation branches).
    newly_discovered = remove_contracts_non_dapp(
        contracts_dapp - contracts_dapp_before, contracts_non_dapp)
    contracts_dapp = remove_contracts_non_dapp(contracts_dapp, contracts_non_dapp)

    return contracts_dapp, newly_discovered


def remove_contracts_non_dapp(set_contracts_lx, contracts_non_dapp):
    """Remove any predefined non-DApp contracts from a contract set.

    Args:
        set_contracts_lx (set of str): contract addresses to filter.
        contracts_non_dapp (set of str): addresses to remove if present.

    Returns:
        set of str: the filtered set.
    """
    set_contracts_lx = set(set_contracts_lx)
    contracts_non_dapp = set(contracts_non_dapp)
    overlap = set_contracts_lx & contracts_non_dapp
    if overlap:
        set_contracts_lx -= contracts_non_dapp
        logger.info(f"Predefined non-DApp contracts excluded: {overlap}")
    return set_contracts_lx
