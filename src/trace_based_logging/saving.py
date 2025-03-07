import os
import pickle
from src.trace_based_logging.logging_config import setup_logging

logger = setup_logging()

def save_trace_data(config, state, dir_path):
    base_contract = state["base_contract"]
    csv_path = os.path.join(dir_path, "resources", f"df_trace_tree_{base_contract}_{config['min_block']}_{config['max_block']}.csv")
    pkl_path = os.path.join(dir_path, "resources", f"df_trace_tree_{base_contract}_{config['min_block']}_{config['max_block']}.pkl")
    state["trace_tree"].to_csv(csv_path)
    state["trace_tree"].to_pickle(pkl_path)

    txt_path = os.path.join(dir_path, "resources", f"contracts_dapp_{base_contract}_{config['min_block']}_{config['max_block']}.txt")
    pkl_contract_path = os.path.join(dir_path, "resources", f"contracts_dapp_{base_contract}_{config['min_block']}_{config['max_block']}.pkl")
    with open(txt_path, "w") as output:
        output.write(str(state["contracts_dapp"]))
    pickle.dump(state["contracts_dapp"], open(pkl_contract_path, 'wb'))
    logger.info("DONE WITH THE DATA EXTRACTION, TRACE FILES SAVED")
