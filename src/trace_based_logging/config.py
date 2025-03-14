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
        "events_dapp", "calls_dapp", "zero_value_calls_dapp",
        "delegatecalls_dapp", "events_non_dapp", "calls_non_dapp",
        "zero_value_calls_non_dapp", "delegatecalls_non_dapp", "etherscan_api_key",
        "list_predefined_non_dapp_contracts", "sensitive_events"
    ]
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required configuration key: {key}")
        if key == "etherscan_api_key" and config[key] == "ETHERSCAN_API_KEY":
            raise ValueError("Please provide your Etherscan API key in the configuration file")

def build_node_url(config):
    return f"{config['protocol']}{config['host']}:{config['port']}"

def initialize_extraction_state(config, utils_module):
    contracts_lx = list(map(utils_module.low, config["list_contracts"]))
    non_dapp_contracts = list(map(utils_module.low, config["list_predefined_non_dapp_contracts"]))
    return {
        "contracts_lx": set(contracts_lx),
        "base_contract": contracts_lx[0],
        "contracts_dapp": set(contracts_lx),
        "all_transactions": set(),
        "trace_tree": None,  
        "non_dapp_contracts": set(non_dapp_contracts)
    }
