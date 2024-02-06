import pm4py
import pandas as pd
import math
import datetime
from matplotlib import pyplot as plt
import numpy as np
import ast
import os
import pickle
import time
import utils
import json

list_contracts_lx = ["0x75228dce4d82566d93068a8d5d49435216551599", "0x57f1c2953630056aaadb9bfbda05369e6af7872b"]
list_contracts_lx = list(map(utils.low, list_contracts_lx))
base_contract = list_contracts_lx[0]
list_contracts_lx = set(list_contracts_lx)
# for Augur, important creations occur around block 5926229
min_block = 5926229
max_block = 11229577


dir_path = os.path.dirname(os.path.realpath(__file__))
path = os.path.join(dir_path, "mappings.json")
try:
    # read json input
    with open(path, "r") as file:
        mappings = json.load(file)
except FileNotFoundError:
    print("The mapping JSON file was not found")
except json.JSONDecodeError:
    print("Error decoding the mapping JSON")


# change the default look-up path to the directory above
dir_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..'))

path = os.path.join(dir_path, "resources", 'df_creations_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
creations = pickle.load(open(path, "rb"))

# Ingest dapp contracts
path = os.path.join(dir_path, "resources", "contracts_dapp_" + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
contracts_dapp = pickle.load(open(path, 'rb'))

######################## REVERTED TRANSACTIONS ########################

# if transaction is reverted, flag
# not all reverted operations of txs are labeled as such, often only one of them is -> propagate the label "reverted" or "out of gas"
# collect all reverted operations and the hashes; use the set of hashes to flag reverted operations later
path = os.path.join(dir_path, "resources", 'df_call_dapp_1209_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.csv')
errors_calls_dapp = pd.read_csv(path, usecols=['error', "hash"])
path = os.path.join(dir_path, "resources", 'df_call_non_dapp_1209_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.csv')
errors_calls_non_dapp = pd.read_csv(path, usecols=['error', "hash"])
path = os.path.join(dir_path, "resources", 'df_delegatecall_dapp_1209_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.csv')
errors_delegatecalls_dapp = pd.read_csv(path, usecols=['error', "hash"])
path = os.path.join(dir_path, "resources", 'df_delegatecall_non_dapp_1209_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.csv')
errors_delegatecalls_non_dapp = pd.read_csv(path, usecols=['error', "hash"])
path = os.path.join(dir_path, "resources", 'df_functions_zero_value_1209_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.csv')
errors_functions_zero_value_dapp = pd.read_csv(path, usecols=['error', "hash"])
path = os.path.join(dir_path, "resources", 'df_creations_1209_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.csv')
errors_creations = pd.read_csv(path, usecols=['error', "hash"])

errors = pd.concat(
    [errors_calls_dapp,
    errors_calls_non_dapp,
    errors_delegatecalls_dapp,
    errors_delegatecalls_non_dapp,
    errors_functions_zero_value_dapp,
    errors_creations
    ]
)

errors.reset_index(inplace=True, drop=True)

mask_reverted = errors["error"].isin([
    "execution reverted", 
    "out of gas",
    "invalid jump destination",
    "contract creation code storage out of gas"
    ])
txs_reverted = set(errors[mask_reverted]["hash"])

# free up memory
del errors_calls_dapp
del errors_calls_non_dapp
del errors_delegatecalls_dapp
del errors_delegatecalls_non_dapp
del errors_functions_zero_value_dapp
del errors_creations
del errors

print("Number of reverted transactions: ", len(txs_reverted))

######################## EVENTS DAPP ########################

path = os.path.join(dir_path, "resources", 'df_events_dapp_1209_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
events_dapp = pickle.load(open(path, "rb"))

events_dapp = utils.initial_transformation_events(events_dapp, True, txs_reverted)

# rename events
events_dapp = utils.rename_attribute(events_dapp, "Activity", "Activity", mappings["event_map_dapp"])

activity_split_candidates = [
    "transfer tokens", 
    "give approval to transfer tokens",
    "mint tokens",
    "burn tokens"
]

events_dapp = utils.create_contract_sensitive_events(events_dapp, mappings, creations, contracts_dapp, activity_split_candidates)

# name token types in tokens minted / transferred / burned
events_dapp = utils.rename_attribute(events_dapp, "tokenType", "tokenType_name", mappings["token_map"])
# add token type names to the activity names
events_dapp = utils.combine_attributes(events_dapp, "Activity", "tokenType_name", "Activity_token_sensitive", ", token type: ", [])

# Propagate the extracted information to all activities with the same 'marketId'
market_info = utils.propagate_extraInfo(events_dapp)
events_dapp = pd.merge(events_dapp, market_info, on='market', how='left')

# Propagate the 'marketType' information to all activities with the same 'marketId'
market_type_info = utils.propagate_marketType(events_dapp)
events_dapp = pd.merge(events_dapp, market_type_info, on='market', how='left', suffixes=('', '_propagated'))


# utils.count_events(events_dapp, "Activity_sensitive")


print("Number of EVENTS DAPP: ", len(events_dapp))

#differentiate: 
#    crowdsourcer factory 1 / 2
#    crowdsourcer 1 / 2
#    disputer 1 / 2



######################## CALLS DAPP ########################
 
path = os.path.join(dir_path, "resources", 'df_call_dapp_1209_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
calls_dapp = pickle.load(open(path, "rb"))

calls_dapp = utils.initial_transformation_calls(calls_dapp, True, txs_reverted)

calls_dapp = utils.rename_attribute(calls_dapp, "Activity", "Activity", mappings["calls_map_dapp"])


# rename events
activity_split_candidates = [
    "call and transfer Ether",
    "call trade with limit"
]

calls_dapp = utils.create_contract_sensitive_events(calls_dapp, mappings, creations, contracts_dapp, activity_split_candidates)

# Propagate the extracted information to all activities with the same 'marketId'
calls_dapp = pd.merge(calls_dapp, market_info, on='market', how='left')

# Propagate the 'marketType' information to all activities with the same 'marketId'
calls_dapp = pd.merge(calls_dapp, market_type_info, on='market', how='left', suffixes=('', '_propagated'))

# utils.count_events(calls_dapp, "Activity_contract_sensitive")

calls_dapp["Activity"].unique()

print("Number of CALLS DAPP: ", len(calls_dapp))

# save
#path = os.path.join(dir_path, "resources", "augur_calls_" + str(min_block) + "_" + str(max_block) + ".csv")
#calls_dapp.to_csv(path)
#path = os.path.join(dir_path, "resources", "augur_calls_" + str(min_block) + "_" + str(max_block) + ".pkl")
#pickle.dump(calls_dapp, open(path, 'wb'))




##################### DELEGATECALL DAPP ########################

path = os.path.join(dir_path, "resources", 'df_delegatecall_dapp_1209_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
delegatecalls_dapp = pickle.load(open(path, "rb"))

delegatecalls_dapp = utils.initial_transformation_calls(delegatecalls_dapp, True, txs_reverted)

delegatecalls_dapp = utils.rename_attribute(delegatecalls_dapp, "Activity", "Activity", mappings["delegatecalls_map_dapp"])

activity_split_candidates = [
    'delegate call to get REP token',
    'delegate call to approve',
    'delegate call to get universe',
    'delegate call to get fee window',
    'delegate call to get stake',
    'delegate call to get payout distribution hash',
    'delegate call to initialize',
    'delegate call to get balance',
    'delegate call to transfer token on behalf of the owner',
    'delegate call to get total supply',
    'delegate call to get market',
    'delegate call to get outcome',
    'delegate call to get allowance',
    'delegate call to transfer token',
    'delegate call to check for container for share token',
    'delegate call to check if container container for reporting participant',
    'delegate call and transfer Ether',
    'delegate call to redeem',
    'delegate call to contribute',
    'delegate call to get size',
    'delegate call to migrate',
    'delegate call to check validity',
    'delegate call to deposit Ether',
    'delegate call to check if designated reporter was correct',
    'delegate call to check if designated reporter showed',
    'delegate call to get payout numerator',
    'delegate call to liquidate losing'
    ]

delegatecalls_dapp = utils.create_contract_sensitive_events(delegatecalls_dapp, mappings, creations, contracts_dapp, activity_split_candidates)

# Propagate the extracted information to all activities with the same 'marketId'
delegatecalls_dapp = pd.merge(delegatecalls_dapp, market_info, on='market', how='left')

# Propagate the 'marketType' information to all activities with the same 'marketId'
delegatecalls_dapp = pd.merge(delegatecalls_dapp, market_type_info, on='market', how='left', suffixes=('', '_propagated'))

#delegatecalls_dapp[~delegatecalls_dapp["market"].isnull()]["tags"]
#delegatecalls_dapp[~delegatecalls_dapp["market"].isnull()]["marketType"]

#utils.count_events(delegatecalls_dapp, "Activity")
print("Number of DELEGATECALLS DAPP: ", len(delegatecalls_dapp))

mask = delegatecalls_dapp["Activity"] == "delegate call to transfer token"
pd.set_option('display.max_columns', 500)
delegatecalls_dapp[mask]


######################## EVENTS NON-DAPP ########################

path = os.path.join(dir_path, "resources", 'df_events_non_dapp_1209_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
events_non_dapp = pickle.load(open(path, "rb"))

events_non_dapp = utils.initial_transformation_events(events_non_dapp, False, txs_reverted)

# rename events
events_non_dapp = utils.rename_attribute(events_non_dapp, "Activity", "Activity", mappings["event_map_non_dapp"])

# utils.count_events(events_non_dapp, "Activity")

print("Number of EVENTS NON-DAPP: ", len(events_non_dapp))

'''
######################## CALLS NON-DAPP ########################

# Most of the calls are from exchanges; those could be differentiated by (batches of) address(es) 
# token trade, token swap, token exchange, buy, etc, is all essentially the same activity: one token for another
# consolidate activities? 

path = os.path.join(dir_path, "resources", 'df_call_non_dapp_1209_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
calls_non_dapp = pickle.load(open(path, "rb"))

calls_non_dapp = utils.initial_transformation_calls(calls_non_dapp, False, txs_reverted)

calls_non_dapp = utils.rename_attribute(calls_non_dapp, "Activity", "Activity", mappings["calls_map_non_dapp"])

#utils.count_events(calls_non_dapp, "Activity")
print("Number of CALLS NON-DAPP: ", len(calls_non_dapp))

######################## DELEGATECALL NON-DAPP ########################

path = os.path.join(dir_path, "resources", 'df_delegatecall_non_dapp_1209_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
delegatecalls_non_dapp = pickle.load(open(path, "rb"))

delegatecalls_non_dapp = utils.initial_transformation_calls(delegatecalls_non_dapp, False, txs_reverted)

delegatecalls_non_dapp = utils.rename_attribute(delegatecalls_non_dapp, "Activity", "Activity", mappings["delegatecalls_map_non_dapp"])

# utils.count_events(delegatecalls_non_dapp, "Activity")

print("Number of DELEGATECALLS NON-DAPP: ", len(delegatecalls_non_dapp))
'''

'''
###################################### ZERO VALUE ######################################

######################## ZERO VALUE CALLS DAPP ########################

path = os.path.join(dir_path, "resources", 'df_call_dapp_zero_value_1209_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
calls_dapp_zero_value = pickle.load(open(path, "rb"))
calls_dapp_zero_value = utils.initial_transformation_calls(calls_dapp_zero_value, False, txs_reverted)

print("Number of ZERO VALUE CALLS DAPP: ", len(calls_dapp_zero_value))
utils.count_events(calls_dapp_zero_value, "Activity")

######################## ZERO VALUE CALLS NON-DAPP ########################

path = os.path.join(dir_path, "resources", 'df_call_non_dapp_zero_value_1209_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
calls_non_dapp_zero_value = pickle.load(open(path, "rb"))
calls_non_dapp_zero_value = utils.initial_transformation_calls(calls_non_dapp_zero_value, False, txs_reverted)

print("Number of ZERO VALUE CALLS NON-DAPP: ", len(calls_non_dapp_zero_value))
utils.count_events(calls_non_dapp_zero_value, "Activity")

######################## ZERO VALUE DELEGATECALLS DAPP ########################

path = os.path.join(dir_path, "resources", 'df_delegatecall_dapp_zero_value_1209_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
delegatecalls_dapp_zero_value = pickle.load(open(path, "rb"))
delegatecalls_dapp_zero_value = utils.initial_transformation_calls(delegatecalls_dapp_zero_value, False, txs_reverted)

print("Number of ZERO VALUE DELEGATECALLS DAPP: ", len(delegatecalls_dapp_zero_value))
utils.count_events(delegatecalls_dapp_zero_value, "Activity")

######################## ZERO VALUE DELEGATECALLS NON-DAPP ########################

path = os.path.join(dir_path, "resources", 'df_delegatecall_non_dapp_zero_value_1209_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
delegatecalls_non_dapp_zero_value = pickle.load(open(path, "rb"))
delegatecalls_non_dapp_zero_value = utils.initial_transformation_calls(delegatecalls_non_dapp_zero_value, False, txs_reverted)

print("Number of ZERO VALUE DELEGATECALLS NON-DAPP: ", len(delegatecalls_non_dapp_zero_value))
utils.count_events(delegatecalls_non_dapp_zero_value, "Activity")
'''

######################## CREATIONS ########################

creations_dapp = creations[creations["to"].isin(contracts_dapp)]
creations_dapp.reset_index(inplace = True, drop = True) 

creations_dapp.loc[creations_dapp, "Activity"] = "create new contract"

creations_non_dapp = creations[~creations["to"].isin(contracts_dapp)]
creations_non_dapp.reset_index(inplace = True, drop = True) 

creations_non_dapp.loc[creations_non_dapp, "Activity"] = "create new contract"

######################## CONSOLIDATE ########################

path = os.path.join(dir_path, "resources", "events_non_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".csv")
events_non_dapp.to_csv(path)
path = os.path.join(dir_path, "resources", "events_non_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".pkl")
pickle.dump(events_non_dapp, open(path, 'wb'))
del events_non_dapp
'''
path = os.path.join(dir_path, "resources", "calls_non_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".csv")
calls_non_dapp.to_csv(path)
path = os.path.join(dir_path, "resources", "calls_non_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".pkl")
pickle.dump(calls_non_dapp, open(path, 'wb'))
del calls_non_dapp

path = os.path.join(dir_path, "resources", "delegatecalls_non_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".csv")
delegatecalls_non_dapp.to_csv(path)
path = os.path.join(dir_path, "resources", "delegatecalls_non_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".pkl")
pickle.dump(delegatecalls_non_dapp, open(path, 'wb'))
del delegatecalls_non_dapp
'''


log_dapp = pd.concat([
    events_dapp, 
    calls_dapp, 
    delegatecalls_dapp,
#    events_non_dapp, 
#    calls_non_dapp, 
#    delegatecalls_non_dapp,
    #calls_dapp_zero_value,
    #calls_non_dapp_zero_value,
    #delegatecalls_dapp_zero_value,
    #delegatecalls_non_dapp_zero_value,
#    creations_non_dapp,
    creations_dapp
    ])

#del events_dapp
del calls_dapp
del delegatecalls_dapp
#del events_non_dapp
#del calls_non_dapp 
#del delegatecalls_non_dapp
#calls_dapp_zero_value,
#calls_non_dapp_zero_value,
#delegatecalls_dapp_zero_value,
#delegatecalls_non_dapp_zero_value,
del creations_dapp
#del creations_non_dapp

path = os.path.join(dir_path, "resources", "events_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".csv")
events_dapp.to_csv(path)
path = os.path.join(dir_path, "resources", "events_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".pkl")
pickle.dump(events_dapp, open(path, 'wb'))
del events_dapp
'''
path = os.path.join(dir_path, "resources", "calls_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".csv")
calls_dapp.to_csv(path)
path = os.path.join(dir_path, "resources", "calls_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".pkl")
pickle.dump(calls_dapp, open(path, 'wb'))
del calls_dapp

path = os.path.join(dir_path, "resources", "delegatecalls_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".csv")
delegatecalls_dapp.to_csv(path)
path = os.path.join(dir_path, "resources", "delegatecalls_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".pkl")
pickle.dump(delegatecalls_dapp, open(path, 'wb'))
del delegatecalls_dapp

path = os.path.join(dir_path, "resources", "creations_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".csv")
creations_dapp.to_csv(path)
path = os.path.join(dir_path, "resources", "creations_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".pkl")
pickle.dump(creations_dapp, open(path, 'wb'))
del creations_dapp

path = os.path.join(dir_path, "resources", "creations_non_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".csv")
creations_non_dapp.to_csv(path)
path = os.path.join(dir_path, "resources", "creations_non_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".pkl")
pickle.dump(creations_non_dapp, open(path, 'wb'))
del creations_non_dapp
'''

log_dapp.reset_index(inplace=True, drop=True)
if "functionName" in log_dapp.columns:
    print("deleting column 'functionName'")
    log_dapp.drop(["functionName"], inplace=True, axis=1)

log_dapp["Activity_raw"] = log_dapp["Activity"]

mask_token = ~log_dapp["Activity_token_sensitive"].isnull()
log_dapp.loc[mask_token, "Activity"] =  log_dapp.loc[mask_token, "Activity_token_sensitive"]

mask_contract = ~log_dapp["Activity_contract_sensitive"].isnull()
log_dapp.loc[mask_contract, "Activity"] = log_dapp.loc[mask_contract, "Activity_contract_sensitive"]

log_dapp.loc[~mask_contract & ~mask_token, "Activity"] = log_dapp.loc[~mask_contract & ~mask_token, "Activity"]


path = os.path.join(dir_path, "resources", "log_augur_" + str(min_block) + "_" + str(max_block) + ".csv")
log_dapp.to_csv(path)
path = os.path.join(dir_path, "resources", "log_augur_" + str(min_block) + "_" + str(max_block) + ".pkl")
pickle.dump(log_dapp, open(path, 'wb'))



print("ENTRIES")
print(len(log_dapp))
list_unique_activities = log_dapp["Activity"].unique()
print("Number of unique activities: ", len(list_unique_activities))
for event in list_unique_activities:
    print(event)

print("ATTRIBUTES")
list_cols = log_dapp.columns.unique()
print("Number of unique attributes: ", len(list_cols))
for col in list_cols:
    print(col)
log_dapp.info()


utils.count_events(log_dapp, "Activity")

len(log_dapp[log_dapp["Activity"]=="create new contract"])

dict_augur_base_log_events = {
    "create market": 2897,
    "finalize market": 2738, 
    "claim trading proceeds": 6050,
    "complete dispute": 782,
    "contribute to dispute": 1602,
    "create dispute": 904,
    "purchase complete sets": 570,
    "redeem as initial reporter": 3260,
    "redeem dispute crowdsourcer": 1412,
    "submit initial report": 2805,
    "transfer market": 1
}

for key,value in dict_augur_base_log_events.items():
    if value == len(log_dapp[log_dapp["Activity"]==key]):
        print(key, "is okay")
    else:
        print(key, "DIFFERENT COUNT")
    


'''
transfer tokens, contract: Root (1) deployment deployment 1
give approval to transfer tokens, contract: Reputation Token 25478
transfer tokens, contract: Root (2) deployment deployment 1
give approval to transfer tokens, contract: Delegator 28556
mint tokens (2), contract: Reputation Token 56389
mint tokens, token type: Reputation Token (REP) 56389
transfer tokens, contract: Reputation Token 433335
transfer tokens (2), token type: Reputation Token (REP) 433335
give approval to transfer tokens, contract: Share Token 7807
mint tokens (2), contract: Delegator 33660
transfer tokens, contract: Delegator 116994
burn tokens (2), contract: Delegator 35111
transfer tokens, contract: Share Token 47877
mint tokens (2), contract: Share Token 26835
transfer tokens (2), token type: Shares Token (SHARE) 47877
mint tokens, token type: Shares Token (SHARE) 26835
mint tokens (2), contract: Fee Window 1173
mint tokens, token type: Fee Window Token (PT) 1173
mint tokens (2), contract: Fee Token 7632
mint tokens, token type: Fee Token (FEE) 7632
burn tokens (2), contract: Share Token 10680
burn tokens, token type: Shares Token (SHARE) 10680
burn tokens (2), contract: Fee Window 961
burn tokens, token type: Fee Window Token (PT) 961
mint tokens (2), contract: Dispute Crowdsourcer 1591
mint tokens, token type: Dispute Crowdsourcer Token 1591
burn tokens (2), contract: Fee Token 5402
burn tokens, token type: Fee Token (FEE) 5402
burn tokens (2), contract: Dispute Crowdsourcer 1412
burn tokens, token type: Dispute Crowdsourcer Token 1412
transfer tokens, contract: Dispute Crowdsourcer 186
transfer tokens (2), token type: Dispute Crowdsourcer Token 186
give approval to transfer tokens, contract: Dispute Crowdsourcer 64
transfer tokens, contract: Fee Window 2
transfer tokens (2), token type: Fee Window Token (PT) 2
'''

pd.set_option('display.max_columns', 200)
mask = events_dapp["Activity"] == "transfer tokens"
events_dapp[mask]

mask = events_dapp["Activity"] == "transferred tokens"
events_dapp[mask]

events_dapp["Activity_token_sensitive"].unique()




# Create a dictionary with all DApp contracts and their names


def low(x):
    return x.lower()
contracts_map, mysterious_contracts = label_contracts_by_relative(creations, contracts_dapp, mappings["factory_contract_map"])

def label_contracts_by_relative(creations, contracts_dapp, factory_contract_map):
    mysterious_contracts = []
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
    try: 
        for contract_factory in contract_relationships:
            if "Factory" in factory_contract_map[low(contract_factory)][-7:]:
                for contract in contract_relationships[low(contract_factory)]:
                    contract_name_map[low(contract)] = factory_contract_map[low(contract_factory)][:-8]
            else:
                for contract in contract_relationships[low(contract_factory)]:
                    contract_name_map[low(contract)] = factory_contract_map[low(contract_factory)] + " deployment"
        #    for contract in contract_relationships[contract_factory]:
    except:
        mysterious_contracts.append(contract_factory)
    return contract_name_map, mysterious_contracts 

mysterious_contracts

contracts_map.update(mappings["factory_contract_map"])
contracts_map.update(mappings["map_specific_known_contracts"])

set(contracts_map.values())

len(contracts_map)

path = os.path.join(dir_path, "resources", "contracts_map_" + str(min_block) + "_" + str(max_block) + ".pkl")
pickle.dump(contracts_map, open(path, 'wb'))

