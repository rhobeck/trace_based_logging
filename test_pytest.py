import src.trace_based_logging.raw_trace_retriever.get_transactions as get_transactions
import src.trace_based_logging.raw_trace_retriever.trace_transformation as trace_transformation
import src.trace_based_logging.raw_trace_retriever.trace_retriever_utils as trace_retriever_utils
import src.trace_based_logging.raw_trace_retriever.create_relations as create_relations
import src.trace_based_logging.trace_decoder.data_preparation as data_preparation
import src.trace_based_logging.trace_decoder.event_decoder as event_decoder
import src.trace_based_logging.log_construction.log_construction_utils as log_construction_utils
import pickle
import os
import pandas as pd
import math
import json
import numpy as np


dir_path = os.path.dirname(os.path.realpath(__file__))

# def test_check_socket():
#     port = 8081
#     port_message = functions_helpers.check_socket("127.0.0.1", port)
#     assert(port_message == "Port is open")

with open('config.json', 'r') as file:
        config = json.load(file)
    
etherscan_api_key = config["etherscan_api_key"]

port = config["port"]
protocol = config["protocol"]
host = config["host"]
node_url = protocol + host + ":" + str(port)

def load_event_definitions(config_file):
    with open(config_file, 'r') as file:
        event_definitions = json.load(file)
    return event_definitions
    

def test_get_transactions():
    # Load correct dataframes for internal and external transactions
    path = os.path.join(dir_path, 'tests', 'test_resources', 'df_txs_lx_normal_0xd5524179cb7ae012f5b642c1d6d700bbaa76b96b_12804576_14355905.pkl')
    df_txs_lx_normal = pickle.load(open(path, 'rb'))
    path = os.path.join(dir_path, 'tests', 'test_resources', 'df_txs_lx_internal_0xd5524179cb7ae012f5b642c1d6d700bbaa76b96b_12804576_14355905.pkl')
    df_txs_lx_internal = pickle.load(open(path, 'rb'))
    
    list_contract_address = ["0xd5524179cb7ae012f5b642c1d6d700bbaa76b96b"]
    min_block = 12804576
    max_block = 14355905
    
    df_normal = get_transactions.get_transactions(list_contract_address, min_block, max_block, internal_flag="normal", etherscan_api_key=etherscan_api_key)
    df_internal = get_transactions.get_transactions(list_contract_address, min_block, max_block, internal_flag="internal", etherscan_api_key=etherscan_api_key)
    
    assert len(df_normal) == len(df_txs_lx_normal) # == 10
    assert len(df_internal) == len(df_txs_lx_internal) # == 10
    assert list(df_normal.keys()) == list(df_txs_lx_normal.keys())
    assert list(df_internal.keys()) == list(df_txs_lx_internal.keys())

    # With multiple contracts as input to the loop
    path = os.path.join(dir_path, 'tests', 'test_resources', 'df_txs_lx_normal_0xd5524179cb7ae012f5b642c1d6d700bbaa76b96b_0x24e2B1d415E6E0d04042eaa45Dc2A08FC33CA6Cd_12804576_14355905.pkl')
    df_txs_lx_normal = pickle.load(open(path, 'rb'))
    path = os.path.join(dir_path, 'tests', 'test_resources', 'df_txs_lx_internal_0xd5524179cb7ae012f5b642c1d6d700bbaa76b96b_0x24e2B1d415E6E0d04042eaa45Dc2A08FC33CA6Cd_12804576_14355905.pkl')
    df_txs_lx_internal = pickle.load(open(path, 'rb'))   
 
    list_contract_address = ["0xd5524179cb7ae012f5b642c1d6d700bbaa76b96b", "0x24e2B1d415E6E0d04042eaa45Dc2A08FC33CA6Cd"]
    min_block = 12804576
    max_block = 14355905
    
    df_normal = get_transactions.get_transactions(list_contract_address, min_block, max_block, internal_flag="normal", etherscan_api_key=etherscan_api_key)
    df_internal = get_transactions.get_transactions(list_contract_address, min_block, max_block, internal_flag="internal", etherscan_api_key=etherscan_api_key)
    
    assert len(df_normal) == len(df_txs_lx_normal) # == 11
    assert len(df_internal) == len(df_txs_lx_internal) # == 11
    assert list(df_normal.keys()) == list(df_txs_lx_normal.keys()) 
    assert list(df_internal.keys()) == list(df_txs_lx_internal.keys())

    min_block = 9999881
    max_block = 10161640
    list_contract_address = ["0x1985365e9f78359a9b6ad760e32412f4a445e862"]
    df_normal = get_transactions.get_transactions(list_contract_address, min_block, max_block, internal_flag="normal", etherscan_api_key=etherscan_api_key)
    assert len(df_normal) == 10001
    
    min_block = 9999881
    max_block = 10304956
    list_contract_address = ["0x1985365e9f78359a9b6ad760e32412f4a445e862"]
    df_normal = get_transactions.get_transactions(list_contract_address, min_block, max_block, internal_flag="normal", etherscan_api_key=etherscan_api_key)
    assert len(df_normal) == 20000


def test_get_transactions_by_events():
    min_block = 11280718
    max_block = 11280718  
    contracts = ["0xc7af99fe5513eb6710e6d5f44f9989da40f27f26"]
    df = get_transactions.get_transactions_by_events(node_url, contracts, min_block, max_block)
    assert df["hash"][0] == "0x0499e012b24bae9941a145cd12d045cf63d8b544d050601b60665ba111310ad4"
    assert len(df) == 1

    min_block = 11280718
    max_block = 11310918  
    contracts = ["0xc7af99fe5513eb6710e6d5f44f9989da40f27f26", "0xb1690c08e213a35ed9bab7b318de14420fb57d8c"]
    df = get_transactions.get_transactions_by_events(node_url, contracts, min_block, max_block)
    assert len(df) == 1174   
    
    
def test_json_retriever():
    # Load a correct, pickled trace for comparison 
    path = os.path.join(dir_path, 'tests', 'test_resources', 'trace_json_lx_0x39a7a29cd1b941424774e0ffa8cc93bcd968f30e3d3d1ee3d7d086916697dc29_erigon2.pkl')
    trace_json_lx_read_erigon2 = pickle.load(open(path, 'rb'))
    
    path = os.path.join(dir_path, 'tests', 'test_resources', 'trace_json_lx_0x39a7a29cd1b941424774e0ffa8cc93bcd968f30e3d3d1ee3d7d086916697dc29_erigon3.pkl')
    trace_json_lx_read_erigon3 = pickle.load(open(path, 'rb'))

    # correct tx hash, all should be fine
    json_dict, json_flag = trace_transformation.json_retriever("0x39a7a29cd1b941424774e0ffa8cc93bcd968f30e3d3d1ee3d7d086916697dc29", node_url)
    assert json_flag == True
    assert json_dict in [trace_json_lx_read_erigon2, trace_json_lx_read_erigon3]
    # case sensitivity, all should be fine
    json_dict, json_flag = trace_transformation.json_retriever("0x39a7a29cd1b941424774e0ffa8cc93bcd968f30e3d3d1ee3d7d086916697DC29", node_url)
    assert json_flag == True
    assert json_dict in [trace_json_lx_read_erigon2, trace_json_lx_read_erigon3]
    # random string as tx hash
    json_dict, json_flag = trace_transformation.json_retriever("string_that_is_no_tx_hash", node_url)
    assert json_flag == False
    assert json_dict not in [trace_json_lx_read_erigon2, trace_json_lx_read_erigon3]
    # length of a hash, but faulty digit (final number: 8 instead of 7)
    json_dict, json_flag = trace_transformation.json_retriever("0xbceb9db10ec228dbef251b1a48bc0b3f03a8d345f7f3b454d8cc1e86f380b6d8", node_url)
    assert json_flag == False
    assert json_dict not in [trace_json_lx_read_erigon2, trace_json_lx_read_erigon3]

def test_txs_to_trace():
    # We test: Does the number of entries in the resulting dataframe reflect the number of entries in the JSON-trace
    path = os.path.join(dir_path, 'tests', 'test_resources', 'df_txs_lx_0x39a7a29cd1b941424774e0ffa8cc93bcd968f30e3d3d1ee3d7d086916697dc29.pkl')
    df_txs_lx = pickle.load(open(path, 'rb'))
    path = os.path.join(dir_path, 'tests', 'test_resources', 'trace_json_lx_0x39a7a29cd1b941424774e0ffa8cc93bcd968f30e3d3d1ee3d7d086916697dc29_erigon2.pkl')
    trace_json_lx_read = pickle.load(open(path, 'rb'))    
    df_trace_lx = trace_transformation.tx_to_trace(df_txs_lx, node_url)
    
    # get the number of referrals between smart contracts (CALL, CREATE, DELEGATECALL, etc.) in the trace
    number_of_referrals = trace_retriever_utils.count_string_occurrences_in_keys(trace_json_lx_read, "type")
    # get the number of events in the trace
    number_of_events = trace_retriever_utils.count_string_occurrences_in_keys(trace_json_lx_read, "topics")
    
    assert len(df_trace_lx) == (number_of_events+number_of_referrals) # == 571
    
    # Check if inserted order is okay
    assert df_trace_lx["tracePos"][0] == 1
    assert df_trace_lx["tracePos"][1] == 2
    assert df_trace_lx["tracePos"][570] == 59
    assert df_trace_lx["tracePos"][568] == 516
    
    # Check if number of events in the trace is okay
    assert len(df_trace_lx[df_trace_lx["address"] == "0x75228dce4d82566d93068a8d5d49435216551599"]) == 18

def test_insert_tracePosDepth():
    mock_trace = {
        "CALL_1":"attribute", "calls":[
            {"CALL_1.1":"attribute"},
            {"CALL_1.2":"attribute","calls":[
                {"CALL_1.2.1":"attribute"},
                {"CALL_1.2.2":"attribute"},
                {"CALL_1.2.3":"attribute","calls":[
                    {"CALL_1.2.3.1":"attribute"},
                    {"CALL_1.2.3.2":"attribute"}
                ],
                "logs": [{"topics":["1.2.3"]},{"topics":["1.2.3"]}]
                }
            ]},
            {"CALL_1.3":"attribute"}
        ],
        "logs": [{"topics":["1"]},{"topics":["1"]}]
    }
    mock_trace_tracePosDepth = trace_transformation.insert_tracePosDepth(mock_trace)
    
    assert mock_trace_tracePosDepth["tracePosDepth"]=="1"
    assert mock_trace_tracePosDepth["calls"][0]["tracePosDepth"]=="1.1"
    assert mock_trace_tracePosDepth["calls"][1]["tracePosDepth"]=="1.2"
    assert mock_trace_tracePosDepth["calls"][2]["tracePosDepth"]=="1.3"
    assert mock_trace_tracePosDepth["calls"][1]["calls"][0]["tracePosDepth"]=="1.2.1"
    assert mock_trace_tracePosDepth["calls"][1]["calls"][1]["tracePosDepth"]=="1.2.2"
    assert mock_trace_tracePosDepth["calls"][1]["calls"][2]["tracePosDepth"]=="1.2.3"
    assert mock_trace_tracePosDepth["calls"][1]["calls"][2]["calls"][0]["tracePosDepth"]=="1.2.3.1"
    assert mock_trace_tracePosDepth["calls"][1]["calls"][2]["calls"][1]["tracePosDepth"]=="1.2.3.2"
    assert mock_trace_tracePosDepth["calls"][1]["calls"][2]["logs"][0]["tracePosDepth"] == "1.2.3.3"
    assert mock_trace_tracePosDepth["calls"][1]["calls"][2]["logs"][1]["tracePosDepth"] == "1.2.3.4"
    assert mock_trace_tracePosDepth["calls"][2]["tracePosDepth"] == "1.3"
    
def test_insert_tracePos():

    mock_trace = {
        "CALL_1":"attribute", "calls":[
            {"CALL_1.1":"attribute"},
            {"CALL_1.2":"attribute","calls":[
                {"CALL_1.2.1":"attribute"},
                {"CALL_1.2.2":"attribute"},
                {"CALL_1.2.3":"attribute","calls":[
                    {"CALL_1.2.3.1":"attribute"},
                    {"CALL_1.2.3.2":"attribute"}
                ],
                "logs": [{"topics":["1.2.3"]},{"topics":["1.2.3"]}]
                }
            ]},
            {"CALL_1.3":"attribute"}
        ],
        "logs": [{"topics":["1"]},{"topics":["1"]}]
    }
    mock_trace_tracePos = trace_transformation.insert_tracePos(mock_trace,trace_pos_counter=[0])
    
    assert mock_trace_tracePos["tracePos"] == 1
    assert mock_trace_tracePos["calls"][0]["tracePos"] == 2
    assert mock_trace_tracePos["calls"][1]["tracePos"] == 3
    assert mock_trace_tracePos["calls"][1]["calls"][0]["tracePos"] == 4
    assert mock_trace_tracePos["calls"][1]["calls"][1]["tracePos"] == 5
    assert mock_trace_tracePos["calls"][1]["calls"][2]["tracePos"] == 6
    assert mock_trace_tracePos["calls"][1]["calls"][2]["calls"][0]["tracePos"] == 7
    assert mock_trace_tracePos["calls"][1]["calls"][2]["calls"][1]["tracePos"] == 8
    assert mock_trace_tracePos["calls"][1]["calls"][2]["logs"][0]["tracePos"] == 9
    assert mock_trace_tracePos["calls"][1]["calls"][2]["logs"][1]["tracePos"] == 10
    assert mock_trace_tracePos["calls"][2]["tracePos"] == 11

'''
def test_insert_eventPos():

    mock_trace = {
        "CALL_1":"attribute", "calls":[
            {"CALL_1.1":"attribute"},
            {"CALL_1.2":"attribute","calls":[
                {"CALL_1.2.1":"attribute"},
                {"CALL_1.2.2":"attribute"},
                {"CALL_1.2.3":"attribute","calls":[
                    {"CALL_1.2.3.1":"attribute"},
                    {"CALL_1.2.3.2":"attribute"}
                ],
                "logs": [{"topics":["1.2.3"]},{"topics":["1.2.3"]}]
                }
            ]},
            {"CALL_1.3":"attribute"}
        ],
        "logs": [{"topics":["1"]},{"topics":["1"]}]
    }
    mock_trace_tracePos = trace_transformation.insert_eventPos(mock_trace)
    
    assert mock_trace_tracePos["calls"][1]["calls"][2]["logs"][0]["eventPos"] == 1
    assert mock_trace_tracePos["calls"][1]["calls"][2]["logs"][1]["eventPos"] == 2
    assert mock_trace_tracePos["logs"][0]["eventPos"] == 1
    assert mock_trace_tracePos["logs"][1]["eventPos"] == 2
'''

def test_remove_predefined_contracts():
    set_contracts_lx = ["0x123...", "0x456...", "0x0000000000000000000000000000000000000000", "0x789..."]
    set_predefined_non_dapp_contracts = ["0x0000000000000000000000000000000000000000", "0xabc...", "0xdef..."]
    filtered_set = create_relations.remove_predefined_contracts(set_contracts_lx, set_predefined_non_dapp_contracts)
    assert filtered_set == {'0x123...', '0x789...', '0x456...'}

 
 ############# MODULE TRACE DECODER ############# 


def test_abi_retrieval():
    path = os.path.join(dir_path, 'tests', 'test_resources', 'addresses_6.pkl')
    addresses = pickle.load(open(path, 'rb'))
    dict_abi = data_preparation.create_abi_dict(addresses, etherscan_api_key)
    #assert(len(non_verified_addresses)==1)
    #assert(len(verified_addresses) == 5)
    assert(len(dict_abi) == 5)


def test_decode_events():
    base_contract = "0xbcc9946143534e28c3bad116cea0f81b9b208799"
    path = os.path.join(dir_path, 'tests', 'test_resources', 'dict_abi_' +  base_contract + '_clean.pkl')
    dict_abi = pickle.load(open(path, 'rb'))
    path = os.path.join(dir_path, 'tests', 'test_resources', 'df_log_' +  base_contract + '.pkl')
    df_log = pickle.load(open(path, 'rb'))
    # format of df_log has changed, after test was originally written (order_in_trace was inserted as a column / attribute)
    df_log["tracePos"] = df_log["order"]
    df_log["tracePosDepth"] = df_log["order"]
    df_log["address"] = df_log["address"].apply(lambda x: str(x).lower() if str(x) != "nan" else x)
    df_log["transactionIndex"] = None
    df_events = data_preparation.decode_events(df_log, dict_abi)
    df_events.reset_index(drop=True, inplace=True)
    # Test for a fallback event / standard event
    assert(df_events["name"][1] == "Approval")
    # Test of an event in the contract specific API
    assert(df_events["name"][33] == "Deposited")
    assert(len(df_events == 40))
    #assert(txs_event_not_decoded == ['0x805b27c880eb35907a2a356dbceb318eec6b596670fd2b5488adca1a9bd7d084'])
    #assert(unknown_event_addresses == {'0x1dd864ed6f291b31c86aaf228db387cd60a20e18'})
    
    
def test_event_decoder():
    
    path = os.path.join(dir_path, 'src', 'trace_based_logging', 'trace_decoder','config_custom_events.json')
    fallback_abis = load_event_definitions(path)
    
    tx_ERC_20_Transfer = "0xc4f4145f215d491be7123beacffe51d3d007a8060aab92826946c0dc744a9349"
    tx_hash = tx_ERC_20_Transfer
    
    path = os.path.join(dir_path, 'tests', 'test_resources', 'df_log_tx_ERC_20_Transfer_'+  tx_hash + '.pkl')
    df_log = pickle.load(open(path, 'rb'))
    
    i = 0
    address = df_log["address"].iloc[i]
    dict_abi = data_preparation.create_abi_dict([address], etherscan_api_key)
    address = df_log["address"].iloc[i]
    topics = df_log["topics"].iloc[i]
    data = df_log["data"].iloc[i]
    decoded_event = event_decoder.event_decoder(address, topics, data, dict_abi, fallback_abis)
    assert(decoded_event["name"] == "Transfer")
    
    i = 1
    address = df_log["address"].iloc[i]
    dict_abi = data_preparation.create_abi_dict([address], etherscan_api_key)
    address = df_log["address"].iloc[i]
    topics = df_log["topics"].iloc[i]
    data = df_log["data"].iloc[i]
    decoded_event = event_decoder.event_decoder(address, topics, data, dict_abi, fallback_abis)
    assert(decoded_event["name"] == "TokensMinted")
    
    i = 2
    address = df_log["address"].iloc[i]
    dict_abi = data_preparation.create_abi_dict([address], etherscan_api_key)
    address = df_log["address"].iloc[i]
    topics = df_log["topics"].iloc[i]
    data = df_log["data"].iloc[i]
    decoded_event = event_decoder.event_decoder(address, topics, data, dict_abi, fallback_abis)
    assert(decoded_event["name"] == "TokenBalanceChanged")

    i = 3
    address = df_log["address"].iloc[i]
    dict_abi = data_preparation.create_abi_dict([address], etherscan_api_key)
    address = df_log["address"].iloc[i]
    topics = df_log["topics"].iloc[i]
    data = df_log["data"].iloc[i]
    decoded_event = event_decoder.event_decoder(address, topics, data, dict_abi, fallback_abis)
    assert(decoded_event["name"] == "Transfer")

    i = 4
    address = df_log["address"].iloc[i]
    dict_abi = data_preparation.create_abi_dict([address], etherscan_api_key)
    address = df_log["address"].iloc[i]
    topics = df_log["topics"].iloc[i]
    data = df_log["data"].iloc[i]
    decoded_event = event_decoder.event_decoder(address, topics, data, dict_abi, fallback_abis)
    assert(decoded_event["name"] == "TokensTransferred")
    
    ########## ERC 20 Approval ##########
    tx_ERC_20_Approval = "0x822ddf5837aee5cb46de3c2c62c0590f6bd81f11e46b1ec96e572e66fb338d25"
    tx_hash = tx_ERC_20_Approval
    
    path = os.path.join(dir_path, 'tests', 'test_resources', 'df_log_tx_ERC_20_Transfer_'+  tx_hash + '.pkl')
    df_log = pickle.load(open(path, 'rb'))
    
    i = 0
    address = df_log["address"].iloc[i]
    dict_abi = data_preparation.create_abi_dict([address], etherscan_api_key)
    address = df_log["address"].iloc[i]
    topics = df_log["topics"].iloc[i]
    data = df_log["data"].iloc[i]
    decoded_event = event_decoder.event_decoder(address, topics, data, dict_abi, fallback_abis)
    assert(decoded_event["name"] == "Approval")
    
    ########## ERC 721 Transfer #########
    tx_ERC_721_Transfer = "0x9e8f42799dffe9f5700e1f871a20a9483e1c84db73c382c31a4412b9a2f83b2b"
    tx_hash = tx_ERC_721_Transfer    
    path = os.path.join(dir_path, 'tests', 'test_resources', 'df_log_tx_ERC_20_Transfer_'+  tx_hash + '.pkl')
    df_log = pickle.load(open(path, 'rb'))

    i = 1
    address = df_log["address"].iloc[i]
    dict_abi = data_preparation.create_abi_dict([address], etherscan_api_key)
    address = df_log["address"].iloc[i]
    topics = df_log["topics"].iloc[i]
    data = df_log["data"].iloc[i]
    decoded_event = event_decoder.event_decoder(address, topics, data, dict_abi, fallback_abis)
    assert(decoded_event["name"] == "Transfer")
       
    # if Event is not in the ABI (provide invalid ABI), the event should be drawn from the ERC-20 standard event
    dict_abi = {address: "rhubarb"}
    decoded_event = event_decoder.event_decoder(address, topics, data, dict_abi, fallback_abis)
    assert(decoded_event["name"] == "Transfer")
      


def test_process_abi():
    path = os.path.join(dir_path, "tests", "test_resources", "df_trace_tree_0x75228dce4d82566d93068a8d5d49435216551599_5937093_7000011.pkl")
    df_log = pickle.load(open(path, "rb"))

    path = os.path.join(dir_path, "tests", "test_resources", "contracts_dapp_0x75228dce4d82566d93068a8d5d49435216551599_5926229_11229573.pkl")
    contracts_dapp = pickle.load(open(path, 'rb'))

    path = os.path.join(dir_path, "tests", "test_resources", "dict_abi_0x75228dce4d82566d93068a8d5d49435216551599_5926229_11229573.pkl")
    dict_abi = pickle.load(open(path, 'rb'))

    df_log = data_preparation.base_transformation(df_log, contracts_dapp)

    contract_address_tmp = df_log["to"][722]#"0x24e2b1d415e6e0d04042eaa45dc2a08fc33ca6cd"
    abi = dict_abi[contract_address_tmp]
    contract = data_preparation.process_abi(abi, contract_address_tmp, node_url)

    input_data = df_log["input"][722]
    func_obj, func_params = contract.decode_function_input(input_data)
    
    assert(str(func_obj) == "<Function publicTradeWithLimit(uint8,address,uint256,uint256,uint256,bytes32,bytes32,bytes32,uint256)>")


def test_function_decoder():
#    dir_path = r"C:\Users\richa\Nextcloud\Cloud Documents\07_Projekte\Process Mining on Blockchain Data\Tracing\trace_based_logging"
    
    path = os.path.join(dir_path, "tests", "test_resources", "df_trace_tree_0x75228dce4d82566d93068a8d5d49435216551599_5937093_7000011.pkl")
    df_log = pickle.load(open(path, "rb"))

    path = os.path.join(dir_path, "tests", "test_resources", "contracts_dapp_0x75228dce4d82566d93068a8d5d49435216551599_5926229_11229573.pkl")
    contracts_dapp = pickle.load(open(path, 'rb'))

    # For establishing proper test data (order_in_trace has been inserted as a proper column in newer version):
    # TODO: update test data -> apply the following lines once an save the new dataframe as pickle
    df_log["order_calls"] = df_log["from"].apply(lambda x: int(x[43:]) if isinstance(x, str) else 0)
    df_log["order_events"] = df_log["address"].apply(lambda x: int(x[43:]) if isinstance(x, str) else 0)
    df_log["tracePos"] = df_log["order_calls"] + df_log["order_events"]
    df_log["tracePosDepth"] = df_log["tracePos"]
    df_log["transactionIndex"] = None
    df_log.drop(["order_calls", "order_events"], axis=1, inplace=True)

    # delete the ordering attachement from "from" and "address"
    df_log["from"] = df_log["from"].apply(lambda x: x[:42] if isinstance(x, str) else np.nan)
    df_log["address"] = df_log["address"].apply(lambda x: x[:42] if isinstance(x, str) else np.nan)

    df_log = data_preparation.base_transformation(df_log, contracts_dapp)
    
    path = os.path.join(dir_path, "tests", "test_resources", "dict_abi_0x75228dce4d82566d93068a8d5d49435216551599_5926229_11229573.pkl")
    dict_abi = pickle.load(open(path, 'rb'))

    logging_string = "DAPP WITH ETHER TRANSFER"
    df_functions = data_preparation.decode_functions(df_log, dict_abi, node_url, ["CALL"], False, logging_string)

    item = df_functions["name"].unique()[2]
    assert(item == "<Function createUniverse(address,address,bytes32)>")

    assert(len(df_functions) == 9395)
    # if len(df_functions) != 9395 then probably faulty error handling

'''
def test_propagate_extraInfo():
    data = {
        'Activity': ['create market', 'update market', 'update market', 'create market', 'update market', "non-market event"],
        'marketId': ['A', 'B', 'A', 'B', 'B', None],
        'extraInfo': [
            '{"longDescription":"entry1A","resolutionSource":"entry2A","tags":"entry3A"}',
            None,
            None,
            '{"longDescription":"entry1B","resolutionSource":"entry2B","tags":"entry3B"}',
            None,
            None
        ],
        "marketType": [1, None, None, 2, None, None]
    }
        
    df = pd.DataFrame(data)
    df = utils.propagate_extraInfo(df)
    assert df["resolutionSource"][0] == "entry2A"
    assert df["longDescription"][3] == "entry1B"
    assert math.isnan(df["resolutionSource"][5])
    
    
def test_propagate_marketType():
    data = {
        'Activity': ['create market', 'update market', 'update market', 'create market', 'update market', "non-market event"],
        'marketId': ['A', 'B', 'A', 'B', 'B', None],
        'extraInfo': [
            '{"longDescription":"entry1A","resolutionSource":"entry2A","tags":"entry3A"}',
            None,
            None,
            '{"longDescription":"entry1B","resolutionSource":"entry2B","tags":"entry3B"}',
            None,
            None
        ],
        "marketType": [1, None, None, 2, None, None]
    }
        
    df = pd.DataFrame(data)
    df = utils.propagate_extraInfo(df)
    df = utils.propagate_marketType(df)
    assert df["marketType_propagated"][2] == 1
    assert math.isnan(df["marketType_propagated"][5])

def test_function():
    # Opening JSON file
    with open(root_path+'\resources\test_0x39a7a29cd1b941424774e0ffa8cc93bcd968f30e3d3d1ee3d7d086916697dc29.json', 'r') as openfile: 
        # Reading from json file
        json_object = json.load(openfile)
    assert functions_trace_tree.function(json_object)
''' 

def testconvert_hex_to_int_valid_hexadecimal_values():
    data = {'gas': ['0x4A817C800', '0x6B49', '0xABC'], 'gasUsed': ['0x5208', '0x6BCD', '0x7F2']}
    df = pd.DataFrame(data)
    expected_output = {'gas': [20000000000, 27465, 2748], 'gasUsed': [21000, 27597, 2034]}
    expected_df = pd.DataFrame(expected_output)

    result_df = data_preparation.convert_hex_to_int(df)
    pd.testing.assert_frame_equal(result_df, expected_df)

def test_convert_hex_to_int_invalid_hexadecimal_values():
    data = {'gas': ['0x4A817C800', '0xXYZ', '0x7F2']}
    df = pd.DataFrame(data)
    expected_output = {'gas': [20000000000, '0xXYZ', 2034]}
    expected_df = pd.DataFrame(expected_output)

    result_df = data_preparation.convert_hex_to_int(df)
    pd.testing.assert_frame_equal(result_df, expected_df)

def test_convert_hex_to_int_missing_values():
    data = {'gas': ['0x4A817C800', None, '0x7F2']}
    df = pd.DataFrame(data)
    expected_output = {'gas': [20000000000, None, 2034]}
    expected_df = pd.DataFrame(expected_output)

    result_df = data_preparation.convert_hex_to_int(df)
    pd.testing.assert_frame_equal(result_df, expected_df)

def test_convert_hex_to_int_non_existent_columns():
    data = {'gas': ['0x4A817C800', '0x6B49', '0xABC']}
    df = pd.DataFrame(data)
    expected_data = {'gas': [20000000000, 27465, 2748]}  
    expected_df = pd.DataFrame(expected_data)

    # Testing a non-existent column should not modify the DataFrame
    result_df = data_preparation.convert_hex_to_int(df, list_of_cols=['gas', 'not_there'])
    pd.testing.assert_frame_equal(result_df, expected_df)

def test_convert_hex_to_int_mixed_conditions():
    data = {'gas': ['0x4A817C800', '0xXYZ', None], 'gasUsed': [None, '0xZZZ', '0x7F2']}
    df = pd.DataFrame(data)
    expected_output = {'gas': [20000000000, '0xXYZ', None], 'gasUsed': [None, '0xZZZ', 2034]}
    expected_df = pd.DataFrame(expected_output)

    result_df = data_preparation.convert_hex_to_int(df)
    pd.testing.assert_frame_equal(result_df, expected_df)