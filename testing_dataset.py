from web3 import Web3
import pickle
import os
import raw_trace_retriever.helpers as helpers
import trace_decoder.event_decoder as event_decoder



# this is a sampling test
# sample transactions are drawn form the log containig events of the DApp
# events emitted during the transaction are queried from a node
# queried events are decoded with contract abis
# number of events per transaction in the log and from the query are compared 

list_contracts_lx = ["0x75228dce4d82566d93068a8d5d49435216551599", "0x57f1c2953630056aaadb9bfbda05369e6af7872b"]
list_contracts_lx = list(map(helpers.low, list_contracts_lx))
base_contract = list_contracts_lx[0]
list_contracts_lx = set(list_contracts_lx)
# for Augur, important creations occur around block 5926229
min_block = 5926229
#max_block = 11229573
max_block = 11229577

dir_path = os.path.dirname(os.path.realpath(__file__))
path = os.path.join(dir_path, "resources", 'df_events_augur' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
events_dapp = pickle.load(open(path, "rb"))

path = os.path.join(dir_path, "resources", 'contracts_dapp_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
contracts_dapp = pickle.load(open(path, "rb"))

path = os.path.join(dir_path, "resources", 'dict_abi_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
dict_abi = pickle.load(open(path, "rb"))

dict_abi_contracts = {key: value for key, value in dict_abi.items() if key in contracts_dapp}


# Connect to a local Ethereum node
web3 = Web3(Web3.HTTPProvider('http://localhost:8081'))

def get_events_by_transaction(transaction_hash):
    # Get the transaction receipt
    receipt = web3.eth.getTransactionReceipt(transaction_hash)

    if receipt:
        # Iterate through the logs in the receipt
        count_decode = 0
        count_no_decode = 0
        for log in receipt["logs"]:
            address = helpers.low(log["address"])
            topics = log["topics"]
            data = log["data"]
            if address in contracts_dapp:
                try:
                    decoded_event = event_decoder.event_decoder(address, topics, data, dict_abi_contracts)
                    count_decode +=1
                except:
                    count_no_decode +=1
            else: 
                count_no_decode +=1
    else:
        print("Transaction receipt not found.")
    return count_decode, count_no_decode



import numpy as np
# from https://www.geeksforgeeks.org/python-generate-random-numbers-within-a-given-range-and-store-in-a-list/
def Rand(start, end, num):
    res = []
 
    for j in range(num):
        res.append(np.random.randint(start, end))
 
    return res
 
 
num = 10000
list_indexes = Rand(0, len(events_dapp), num)
dict_check = {}
dict_dataset = {}
for i in list_indexes:
    transaction_hash = events_dapp["txHash"][i]
    number_of_events_dataset = len(events_dapp[events_dapp["txHash"] == transaction_hash])
    dict_dataset[transaction_hash] = number_of_events_dataset

    count_decode, count_no_decode = get_events_by_transaction(transaction_hash)
    dict_check[transaction_hash] = count_decode

# in part from https://stackoverflow.com/questions/32815640/how-to-get-the-difference-between-two-dictionaries-in-python
set_check = set(dict_check.items())
set_dataset = set(dict_dataset.items())
set_compare = set_check ^ set_dataset
if len(set_compare) == 0:
    print("All good with the dataset")
else: 
    print("There are differences in the data. Check set_compare.")
