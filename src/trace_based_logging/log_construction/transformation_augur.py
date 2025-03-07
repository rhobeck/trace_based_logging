import pandas as pd
import numpy as np
import os
import pickle
import utils
import address_classification
import json
from web3 import Web3

dir_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..'))


def load_config(config_path):
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config

def build_node_url(config):
    """Construct the Ethereum node URL from the config."""
    return f"{config['protocol']}{config['host']}:{config['port']}"


config = load_config(os.path.join(dir_path, 'config.json'))

list_contracts = config["list_contracts"]
base_contract = list_contracts[0]
list_contracts = set(list_contracts)
# for Augur, important creations occur around block 5926229
min_block = config["min_block"]
max_block = config["max_block"]
# min_block = 5926229
# max_block = 11229577


log_folder = "log_1029"
sensitive_events = False

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

len(contracts_dapp)

######################## REVERTED TRANSACTIONS ########################

# if transaction is reverted, flag
# not all reverted operations of txs are labeled as such, often only one of them is -> propagate the label "reverted" or "out of gas"
# collect all reverted operations and the hashes; use the set of hashes to flag reverted operations later

path = os.path.join(dir_path, "resources", 'df_trace_tree_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.csv')
errors = pd.read_csv(path, usecols=["error", "hash"])

errors.reset_index(inplace=True, drop=True)

mask_reverted = errors["error"].isin([
    'out of gas', 
    'invalid jump destination',
    'execution reverted',
    'write protection',
    'invalid opcode: INVALID',
    'contract creation code storage out of gas'
    ])
txs_reverted = set(errors[mask_reverted]["hash"])

# free up memory
'''
del errors_events_dapp
del errors_events_non_dapp
del errors_calls_dapp_with_ether_transfer,
del errors_calls_with_ether_transfer_non_dapp,
del errors_delegatecalls_dapp,
del errors_delegatecalls_non_dapp,
del errors_calls_with_no_ether_transfer_dapp,
del errors_calls_with_no_ether_transfer_non_dapp,
del errors_creations
'''
del errors

print("Number of reverted transactions: ", len(txs_reverted))

######################## EVENTS DAPP ########################

path = os.path.join(dir_path, "resources", 'df_events_dapp_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
events_dapp = pickle.load(open(path, "rb"))

events_dapp = utils.initial_transformation_events(events_dapp, True, txs_reverted)

# rename events
events_dapp = utils.rename_attribute(events_dapp, "Activity", "Activity", mappings["event_map_dapp"])

# re-label contracts 
events_dapp = utils.label_contracts(events_dapp, mappings, creations, contracts_dapp)

# name token types in tokens minted / transferred / burned
events_dapp = utils.rename_attribute(events_dapp, "tokenType", "tokenType_name", mappings["token_map"])

if sensitive_events == True:
    activity_split_candidates = [
        "transfer tokens", 
        "give approval to transfer tokens",
        "mint tokens",
        "burn tokens"
    ]

    events_dapp = utils.create_contract_sensitive_events(events_dapp, mappings, creations, contracts_dapp, activity_split_candidates)

    
    # add token type names to the activity names
    events_dapp = utils.combine_attributes(events_dapp, "Activity", "tokenType_name", "Activity_token_sensitive", ", token type: ", [])

    # Propagate the extracted information to all activities with the same 'marketId'
    market_info = utils.propagate_extraInfo(events_dapp)
    events_dapp = pd.merge(events_dapp, market_info, on='market', how='left')

    # Propagate the 'marketType' information to all activities with the same 'marketId'
    market_type_info = utils.propagate_marketType(events_dapp)
    events_dapp = pd.merge(events_dapp, market_type_info, on='market', how='left', suffixes=('', '_propagated'))

else: 
    #print("Sensitive events were not created. Reason: sensitive_events-flag == false")
    print("Sensitive events were not created. Reason: sensitive_events-flag == false")

# utils.count_events(events_dapp, "Activity_sensitive")


print(f"Number of EVENTS DAPP: {len(events_dapp)} -- Now saving ...")

file_name_snipped = "events_dapp_"
path = os.path.join(dir_path, "resources", log_folder, file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".csv")
events_dapp.to_csv(path)
path = os.path.join(dir_path, "resources", log_folder, file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
pickle.dump(events_dapp, open(path, 'wb'))

del events_dapp

#differentiate: 
#    crowdsourcer factory 1 / 2
#    crowdsourcer 1 / 2
#    disputer 1 / 2



######################## CALLS DAPP ########################

path = os.path.join(dir_path, "resources", 'df_call_dapp_with_ether_transfer_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
calls_dapp = pickle.load(open(path, "rb"))

calls_dapp = utils.initial_transformation_calls(calls_dapp, True, txs_reverted)

calls_dapp = utils.rename_attribute(calls_dapp, "Activity", "Activity", mappings["calls_map_dapp"])

calls_dapp = utils.label_contracts(calls_dapp, mappings, creations, contracts_dapp)

# convert hex values
for hexCol in ["orderId", 'betterOrderId', 'worseOrderId', 'tradeGroupId']:
    calls_dapp[hexCol] = calls_dapp[hexCol].apply(lambda x: "0x" + x.hex() if pd.notnull(x) else np.nan)
    #print(df_calls[hexCol].unique())

if sensitive_events == True:
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

else: 
    print("Sensitive events were not created. Reason: sensitive_events-flag == false")

#calls_dapp["Activity"].unique()

print(f"Number of CALLs DAPP: {len(calls_dapp)} -- Now saving ...")

file_name_snipped = "calls_dapp_"
path = os.path.join(dir_path, "resources", log_folder, file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".csv")
calls_dapp.to_csv(path)
path = os.path.join(dir_path, "resources", log_folder, file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
pickle.dump(calls_dapp, open(path, 'wb'))
del calls_dapp



##################### DELEGATECALL DAPP ########################

path = os.path.join(dir_path, "resources", 'df_delegatecall_dapp_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
delegatecalls_dapp = pickle.load(open(path, "rb"))

delegatecalls_dapp = utils.initial_transformation_calls(delegatecalls_dapp, True, txs_reverted)

delegatecalls_dapp = utils.rename_attribute(delegatecalls_dapp, "Activity", "Activity", mappings["delegatecalls_map_dapp"])

delegatecalls_dapp = utils.label_contracts(delegatecalls_dapp, mappings, creations, contracts_dapp)


for hexCol in ["orderId", 'betterOrderId', 'worseOrderId', 'tradeGroupId']:
    delegatecalls_dapp[hexCol] = delegatecalls_dapp[hexCol].apply(lambda x: "0x" + x.hex() if pd.notnull(x) else np.nan)
    print(delegatecalls_dapp[hexCol].unique())


if sensitive_events == True:
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

else: 
    print("Sensitive events were not created. Reason: sensitive_events-flag == false")

utils.count_events(delegatecalls_dapp, "Activity")

print(f"Number of DELEGATECALLs DAPP: {len(delegatecalls_dapp)} -- Now saving ...")

file_name_snipped = "delegatecalls_dapp_"
path = os.path.join(dir_path, "resources", log_folder, file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".csv")
delegatecalls_dapp.to_csv(path)
path = os.path.join(dir_path, "resources", log_folder, file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
pickle.dump(delegatecalls_dapp, open(path, 'wb'))
del delegatecalls_dapp

#mask = delegatecalls_dapp["Activity"] == "delegate call to transfer token"
#pd.set_option('display.max_columns', 500)
#delegatecalls_dapp[mask]


######################## EVENTS NON-DAPP ########################
'''
path = os.path.join(dir_path, "resources", 'df_events_non_dapp_1209_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
events_non_dapp = pickle.load(open(path, "rb"))

events_non_dapp = utils.initial_transformation_events(events_non_dapp, False, txs_reverted)

# rename events
events_non_dapp = utils.rename_attribute(events_non_dapp, "Activity", "Activity", mappings["event_map_non_dapp"])

# utils.count_events(events_non_dapp, "Activity")

print("Number of EVENTS NON-DAPP: ", len(events_non_dapp))


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


###################################### ZERO VALUE ######################################

######################## ZERO VALUE CALLS DAPP ########################


path = os.path.join(dir_path, "resources", 'df_call_dapp_with_no_ether_transfer_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
calls_dapp_zero_value = pickle.load(open(path, "rb"))

calls_dapp_zero_value.reset_index(drop=True, inplace=True)
calls_dapp_zero_value = utils.initial_transformation_calls(calls_dapp_zero_value, True, txs_reverted)

calls_dapp_zero_value = utils.rename_attribute(calls_dapp_zero_value, "Activity", "Activity", mappings["calls_zero_value_map_dapp"])

calls_dapp_zero_value = utils.label_contracts(calls_dapp_zero_value, mappings, creations, contracts_dapp)

for hexCol in ["orderId", 'betterOrderId', 'worseOrderId', 'tradeGroupId']:
    calls_dapp_zero_value[hexCol] = calls_dapp_zero_value[hexCol].apply(lambda x: "0x" + x.hex() if pd.notnull(x) else np.nan)
    print(calls_dapp_zero_value[hexCol].unique())

if sensitive_events == True:
    # rename events
    activity_split_candidates = [
        "call to set controller",
        "call to initialize crowdsourcer",
        "call to check if initialization happened",
        "call to initialize",
        "call to create",
        "call to get dispute round duration in seconds",
        "call to check if fork happened",
        "call to get REP token",
        "call to create REP token",
        "call to get parent universe",
        "call to log REP token transferred",
        "call to log fee window creation",
        "call to get timestamp",
        "call to create fee token",
        "call to create map",
        "call to create share token",
        "call to check if known universe",
        "call to check if container market",
        "call to create order",
        "call for trusted transfer",
        "call to find bounding orders",
        "call to log order creation",
        "call to fill order",
        "call to buy complete sets",
        "call to log order filling",
        "call to increment open interest",
        "call to log share tokens transfer",
        "call to log share token minted",
        "call to check if container for share token",
        "call to check if container for fee window",
        "call to cancel order",
        "call to lor order cancellation",
        "call to fill best order",
        "call to get or cache designated reporter stake",
        "call to get or create next fee window",
        "call to check if container for reporting participant",
        "call to log fee token minted",
        "call to check if container for fee token",
        "call to initiate a trade",
        "call to sell complete sets",
        "call to decrement open interest",
        "call to get or cache reporting fee divisor",
        "call to get REP price in Atto ETH",
        "call to log share token burned",
        "call to buy participation token",
        "call to trade with limit",
        "call to fill best order with limit",
        "call to sell complete sets (public)",
        "call to log complete sets sold",
        "call to create dispute crowdsourcer",
        "call to notify about dispute crowdsourcer creation",
        "call to log dispute crowdsourcer contribution",
        "call to get dispute threshold for fork",
        "call to log dispute crowdsourcer completion",
        "call to log dispute crowdsourcer tokens minted",
        "call to get forking market",
        "call to decrement open interest from market",
        "call to log market finalization",
        "call to get or create fee window before",
        "call to log fee token burning",
        "call to claim trading proceeds",
        "call to log trading proceeds claim",
        "call to log dispute crowdsourcer tokens burned",
        "call to log dispute crowdsourcer redemption",
        "call to log complete set purchase",
        "call to log dispute crowdsourcer tokens transfer",
        "call to contribute",
        "call to check if disputed",
        "call to get REP",
        "call to dispute",
        "call to withdraw proceeds",
        "call to check if finalized",
        "call to get dispute token address",
        "call to approve manager to spend dispute tokens",
        "call to finalize",
        "call to withdraw fees",
        "call to get contract fee receiver",
        "call fee receiver",
        "call to withdraw contribution",
        "call to get total contribution",
        "call to get total fees offered",
        "call to buy complete sets with cash (public)",
        "call to buy (public)",
        "call to transfer ownership",
        "call to set REP price in Atto ETH",
        "call to approve"
    ]

    calls_dapp_zero_value = utils.create_contract_sensitive_events(calls_dapp_zero_value, mappings, creations, contracts_dapp, activity_split_candidates)

    # Propagate the extracted information to all activities with the same 'marketId'
    calls_dapp_zero_value = pd.merge(calls_dapp_zero_value, market_info, on='market', how='left')

    # Propagate the 'marketType' information to all activities with the same 'marketId'
    calls_dapp_zero_value = pd.merge(calls_dapp_zero_value, market_type_info, on='market', how='left', suffixes=('', '_propagated'))

# utils.count_events(calls_dapp, "Activity_contract_sensitive")

    calls_dapp_zero_value["Activity_raw"] = calls_dapp_zero_value["Activity"]

    mask_token = ~calls_dapp_zero_value["Activity_token_sensitive"].isnull()
    calls_dapp_zero_value.loc[mask_token, "Activity"] =  calls_dapp_zero_value.loc[mask_token, "Activity_token_sensitive"]

    mask_contract = ~calls_dapp_zero_value["Activity_contract_sensitive"].isnull()
    calls_dapp_zero_value.loc[mask_contract, "Activity"] = calls_dapp_zero_value.loc[mask_contract, "Activity_contract_sensitive"]

    calls_dapp_zero_value.loc[~mask_contract & ~mask_token, "Activity"] = calls_dapp_zero_value.loc[~mask_contract & ~mask_token, "Activity"]

else: 
    print("Sensitive events were not created. Reason: sensitive_events-flag == False")

calls_dapp_zero_value["Activity"].unique()

calls_dapp_zero_value.reset_index(inplace=True, drop=True)
if "functionName" in calls_dapp_zero_value.columns:
    print("deleting column 'functionName'")
    calls_dapp_zero_value.drop(["functionName"], inplace=True, axis=1)


print(f"Number of ZERO VALUE CALLS DAPP: {len(calls_dapp_zero_value)} -- Now saving ...")

file_name_snipped = "calls_dapp_zero_value_"
path = os.path.join(dir_path, "resources", log_folder, file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".csv")
calls_dapp_zero_value.to_csv(path)
path = os.path.join(dir_path, "resources", log_folder, file_name_snipped + base_contract + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
pickle.dump(calls_dapp_zero_value, open(path, 'wb'))

utils.count_events(calls_dapp_zero_value, "Activity")

del calls_dapp_zero_value

'''
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

######################## CREATIONS DAPP ########################

creations_dapp = creations[creations["to"].isin(contracts_dapp)]
creations_dapp.reset_index(inplace = True, drop = True) 

creations_dapp["dapp"] = True

creations_dapp["Activity"] = "create new contract"

creations_dapp["txHash"] = creations_dapp["hash"]    
creations_dapp.drop('hash', inplace=True, axis=1)

print(f"Number of CREATEs DAPP: {len(creations_dapp)} -- Now saving ...")

file_name_snipped = "creations_dapp"
path = os.path.join(dir_path, "resources", log_folder, file_name_snipped + "_" + str(min_block) + "_" + str(max_block) + ".csv")
creations_dapp.to_csv(path)
path = os.path.join(dir_path, "resources", log_folder, file_name_snipped  + "_" + str(min_block) + "_" + str(max_block) + ".pkl")
pickle.dump(creations_dapp, open(path, 'wb'))

'''
######################## CREATIONS NON-DAPP ########################

creations_non_dapp = creations[~creations["to"].isin(contracts_dapp)]
creations_non_dapp.reset_index(inplace = True, drop = True) 

creations_non_dapp.loc[creations_non_dapp, "Activity"] = "create new contract"
'''

######################## CONSOLIDATE ########################
'''
# consolidate log data into one dataframe (if possible)
print("Starting to concatenate.")
log_dapp = calls_dapp_zero_value.copy()
del calls_dapp_zero_value
print("+zero-value calls")
log_dapp = pd.concat([log_dapp, delegatecalls_dapp], ignore_index=True)
del delegatecalls_dapp
print("+delegatecalls")
log_dapp = pd.concat([log_dapp, calls_dapp], ignore_index=True)
del calls_dapp
print("+calls")
log_dapp = pd.concat([log_dapp, events_dapp], ignore_index=True)
del events_dapp
print("+events")
log_dapp = pd.concat([log_dapp, creations_dapp], ignore_index=True)
del creations_dapp
print("+ creations")


path = os.path.join(dir_path, "resources", "log_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".csv")
log_dapp.to_csv(path)
path = os.path.join(dir_path, "resources", "log_dapp_augur_" + str(min_block) + "_" + str(max_block) + ".pkl")
pickle.dump(log_dapp, open(path, 'wb'))
del events_dapp
'''

#################### ACCOUNT DICTIONARY ####################  
# get the port to the Etherem node for querying addresses
dir_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..'))
path = os.path.join(dir_path, 'config.json')

with open(path, 'r') as file:
    config = json.load(file)
port = config["port"]
protocol = config["protocol"]
host = config["host"]
node_url = protocol + host + ":" + str(port)

address_dict = address_classification.create_address_dict(base_contract, log_folder, dir_path, min_block, max_block, contracts_dapp, node_url, creations, mappings)

path = os.path.join(dir_path, "resources", log_folder, "address_dict_" + str(min_block) + "_" + str(max_block) + ".pkl")
pickle.dump(address_dict, open(path, 'wb'))


# Sanity check: All events from the ELF log included in extracted log? 
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
    if value == len(events_dapp[events_dapp["Activity"]==key]):
        print(key, "is okay")
    else:
        print(key, "DIFFERENT COUNT")
        





################### Log construction ###################

import pandas as pd
import pyarrow.parquet as pq
from pm4py.objects.ocel.obj import OCEL

considered_datasets = ["calls_dapp.parquet", "creations_dapp.parquet", "events_dapp.parquet", "delegatecalls_dapp.parquet", "calls_dapp_zero_value.parquet"]

overall_dataframe = []

for ds in considered_datasets:
    parquet_file = pq.ParquetFile(ds)
    column_names = set(parquet_file.schema.names)
    desired_columns = {"timeStamp", "Activity", "tracePos", "sender", "target", "market", "marketType", "address", "token", "contract_name", "tokenType_name", "orderId", "orderType"}

    dataframe = pd.read_parquet(ds, columns=sorted(list(column_names.intersection(desired_columns))))
    dataframe.dropna(how='all', axis=1, inplace=True)

    overall_dataframe.append(dataframe)

overall_dataframe = pd.concat(overall_dataframe)

objects = []

eoa = sorted(list(set(overall_dataframe.dropna(subset=["sender"])["sender"].unique()).union(set(overall_dataframe.dropna(subset=["target"])["target"].unique()))))
eoa = pd.DataFrame({"ocel:oid": eoa})
eoa["ocel:oid"] = "EOA_" + eoa["ocel:oid"]
eoa["ocel:type"] = "EOA"
eoa["category"] = "EOA"
objects.append(eoa)

tokens = overall_dataframe.dropna(subset=["token"]).groupby("token")["tokenType_name"].first().reset_index()
tokens.rename(columns={"token": "ocel:oid", "tokenType_name": "category"}, inplace=True)
tokens["ocel:oid"] = "TOKEN_" + tokens["ocel:oid"]
tokens["ocel:type"] = "TOKEN"
tokens["category"] = tokens["category"].astype('string')
objects.append(tokens)

orders = overall_dataframe.dropna(subset=["orderId"]).groupby("orderId")["orderType"].first().reset_index()
orders.rename(columns={"orderId": "ocel:oid", "orderType": "category"}, inplace=True)
orders["ocel:oid"] = "ORDER_" + orders["ocel:oid"]
orders["ocel:type"] = "ORDER"
orders["category"] = orders["category"].astype('string')
objects.append(orders)

contracts = overall_dataframe.dropna(subset=["address"]).groupby("address")["contract_name"].first().reset_index()
contracts.rename(columns={"address": "ocel:oid", "contract_name": "category"}, inplace=True)
contracts["ocel:oid"] = "CONTRACT_" + contracts["ocel:oid"]
contracts["ocel:type"] = "CONTRACT"
contracts["category"] = contracts["category"].astype('string')
objects.append(contracts)

markets = overall_dataframe.dropna(subset=["market"]).groupby("market")["marketType"].first().reset_index()
markets.rename(columns={"market": "ocel:oid", "marketType": "category"}, inplace=True)
markets["ocel:oid"] = "MARKET_" + markets["ocel:oid"]
markets["ocel:type"] = "MARKET"
markets["category"] = markets["category"].astype('string')
objects.append(markets)

objects = pd.concat(objects)
del eoa
del tokens
del orders
del contracts
del markets
print(objects)

overall_dataframe.sort_values(["timeStamp", "tracePos"], inplace=True)
overall_dataframe.rename(columns={"Activity": "ocel:activity", "timeStamp": "ocel:timestamp"}, inplace=True)
overall_dataframe["ocel:eid"] = "EID_" + overall_dataframe.index.astype(str)

e2o = []

eoa_from = overall_dataframe[["tracePos", "ocel:eid", "ocel:activity", "ocel:timestamp", "sender"]].dropna(how='any')
eoa_from.rename(columns={"sender": "ocel:oid"}, inplace=True)
eoa_from["ocel:oid"] = "EOA_" + eoa_from["ocel:oid"]
eoa_from["ocel:type"] = "EOA"
eoa_from["ocel:qualifier"] = "EOA_FROM"
e2o.append(eoa_from)

eoa_to = overall_dataframe[["tracePos", "ocel:eid", "ocel:activity", "ocel:timestamp", "target"]].dropna(how='any')
eoa_to.rename(columns={"target": "ocel:oid"}, inplace=True)
eoa_to["ocel:oid"] = "EOA_" + eoa_to["ocel:oid"]
eoa_to["ocel:type"] = "EOA"
eoa_to["ocel:qualifier"] = "EOA_TO"
e2o.append(eoa_to)

tokens = overall_dataframe[["tracePos", "ocel:eid", "ocel:activity", "ocel:timestamp", "token"]].dropna(how='any')
tokens.rename(columns={"token": "ocel:oid"}, inplace=True)
tokens["ocel:oid"] = "TOKEN_" + tokens["ocel:oid"]
tokens["ocel:type"] = "TOKEN"
tokens["ocel:qualifier"] = "TOKEN"
e2o.append(tokens)

orders = overall_dataframe[["tracePos", "ocel:eid", "ocel:activity", "ocel:timestamp", "orderId"]].dropna(how='any')
orders.rename(columns={"orderId": "ocel:oid"}, inplace=True)
orders["ocel:oid"] = "ORDER_" + orders["ocel:oid"]
orders["ocel:type"] = "ORDER"
orders["ocel:qualifier"] = "ORDER"
e2o.append(orders)

contracts = overall_dataframe[["tracePos", "ocel:eid", "ocel:activity", "ocel:timestamp", "address"]].dropna(how='any')
contracts.rename(columns={"address": "ocel:oid"}, inplace=True)
contracts["ocel:oid"] = "CONTRACT_" + contracts["ocel:oid"]
contracts["ocel:type"] = "CONTRACT"
contracts["ocel:qualifier"] = "CONTRACT"
e2o.append(contracts)

markets = overall_dataframe[["tracePos", "ocel:eid", "ocel:activity", "ocel:timestamp", "market"]].dropna(how='any')
markets.rename(columns={"market": "ocel:oid"}, inplace=True)
markets["ocel:oid"] = "MARKET_" + markets["ocel:oid"]
markets["ocel:type"] = "MARKET"
markets["ocel:qualifier"] = "MARKET"
e2o.append(markets)

e2o = pd.concat(e2o)
del eoa_from
del eoa_to
del tokens
del orders
del contracts
del markets
e2o.sort_values(["ocel:timestamp", "tracePos", "ocel:oid"], inplace=True)
del e2o["tracePos"]
#e2o = e2o.groupby(["ocel:eid", "ocel:oid"]).first().reset_index()
print(e2o)

events = overall_dataframe[["tracePos", "ocel:eid", "ocel:activity", "ocel:timestamp"]].dropna(how='any')
events.sort_values(["ocel:timestamp", "tracePos"], inplace=True)
del events["tracePos"]
print(events)

objects.to_parquet("ocel_objects.parquet", index=False)
events.to_parquet("ocel_events.parquet", index=False)
e2o.to_parquet("ocel_e2o.parquet", index=False)


ocel = OCEL(events=pd.read_parquet("ocel_events.parquet"), objects=pd.read_parquet("ocel_objects.parquet"), relations=pd.read_parquet("ocel_e2o.parquet"))
print(ocel)










