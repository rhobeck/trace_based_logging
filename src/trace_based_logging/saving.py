import os
import pickle
from src.trace_based_logging.logging_config import setup_logging

logger = setup_logging()

def folder_set_up(dir_path, config):
    if not os.path.exists(os.path.join(dir_path, "resources", config["log_folder"])):
        os.makedirs(os.path.join(dir_path, "resources", config["log_folder"]))
    
    for folder in ["extraction", "decoding", "transformation"]:
        if not os.path.exists(os.path.join(dir_path, "resources", config["log_folder"], folder)):
            os.makedirs(os.path.join(dir_path, "resources", config["log_folder"], folder))

def save_trace_data(config, state, dir_path):        
    base_contract = state["base_contract"]
    csv_path = os.path.join(dir_path, "resources", config["log_folder"], "extraction", f"df_trace_tree_{base_contract}_{config['min_block']}_{config['max_block']}.csv")
    pkl_path = os.path.join(dir_path, "resources", config["log_folder"], "extraction", f"df_trace_tree_{base_contract}_{config['min_block']}_{config['max_block']}.pkl")
    state["trace_tree"].to_csv(csv_path)
    state["trace_tree"].to_pickle(pkl_path)

    txt_path = os.path.join(dir_path, "resources", config["log_folder"], "extraction", "contracts_dapp_{base_contract}_{config['min_block']}_{config['max_block']}.txt")
    pkl_contract_path = os.path.join(dir_path, "resources", config["log_folder"], "extraction", f"contracts_dapp_{base_contract}_{config['min_block']}_{config['max_block']}.pkl")
    with open(txt_path, "w") as output:
        output.write(str(state["contracts_dapp"]))
    pickle.dump(state["contracts_dapp"], open(pkl_contract_path, 'wb'))
    logger.info("DONE WITH THE DATA EXTRACTION, TRACE FILES SAVED")
