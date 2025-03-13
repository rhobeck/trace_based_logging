import json
import os
from src.trace_based_logging.logging_config import setup_logging

logger = setup_logging()

def load_config(config_path):
    try:
        with open(config_path, 'r') as file:
            config = json.load(file)
        validate_config(config)
        return config
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Error loading configuration file: {e}")
        raise

def validate_config(config):
    required_keys = [
        "port", "protocol", "host", "list_contracts", "min_block", "max_block",
        "extract_normal_transactions", "extract_internal_transactions", "extract_transactions_by_events",
        "dapp_decode_events", "dapp_decode_calls_with_ether_transfer", "dapp_decode_calls_with_no_ether_transfer",
        "dapp_decode_delegatecalls", "non_dapp_decode_events", "non_dapp_decode_calls_with_ether_transfer",
        "non_dapp_decode_calls_with_no_ether_transfer", "non_dapp_decode_delegatecalls", "etherscan_api_key",
        "list_predefined_non_dapp_contracts"
    ]
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required configuration key: {key}")
        if key == "etherscan_api_key" and config[key] == "ETHERSCAN_API_KEY":
            raise ValueError("Please provide your Etherscan API key in the configuration file")

def build_node_url(config):
    return f"{config['protocol']}{config['host']}:{config['port']}"

def initialize_extraction_state(config, utils_module):
    # Use the low() function from your raw_trace_retriever.utils (here imported as utils_module)
    contracts_lx = list(map(utils_module.low, config["list_contracts"]))
    non_dapp_contracts = list(map(utils_module.low, config["list_predefined_non_dapp_contracts"]))
    return {
        "contracts_lx": set(contracts_lx),
        "base_contract": contracts_lx[0],
        "contracts_dapp": set(contracts_lx),
        "all_transactions": set(),
        "trace_tree": None,  # Will be a pandas DataFrame later
        "non_dapp_contracts": set(non_dapp_contracts)
    }
