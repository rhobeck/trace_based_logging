import json
import os
from src.trace_based_logging.logging_config import setup_logging

logger = setup_logging()

def load_config(config_path):
    try:
        with open(config_path, 'r') as file:
            nested_config = json.load(file)
        # Transform the nested configuration into the flat structure expected by the rest of the code.
        config = transform_config(nested_config)
        validate_config(config)
        return config
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Error loading configuration file: {e}")
        raise

def transform_config(nested_config):
    flat_config = {}
    # Ethereum node configuration
    node = nested_config.get("ethereum_node", {})
    flat_config["port"] = node.get("port")
    flat_config["protocol"] = node.get("protocol")
    flat_config["host"] = node.get("host")
    
    # Contracts configuration
    contracts = nested_config.get("contracts", {})
    flat_config["contracts_dapp"] = contracts.get("dapp", [])
    flat_config["contracts_non_dapp"] = contracts.get("non_dapp", [])
    
    # Block range
    block_range = nested_config.get("block_range", {})
    flat_config["min_block"] = block_range.get("min_block")
    flat_config["max_block"] = block_range.get("max_block")
    
    # Extraction settings
    extraction = nested_config.get("extraction", {})
    flat_config["extract_normal_transactions"] = extraction.get("normal_transactions")
    flat_config["extract_internal_transactions"] = extraction.get("internal_transactions")
    flat_config["extract_transactions_by_events"] = extraction.get("transactions_by_events")
    flat_config["etherscan_api_key"] = extraction.get("etherscan_api_key")
    
    # Output settings for dapp
    output = nested_config.get("output", {})
    dapp_output = output.get("dapp", {})
    flat_config["dapp_events"] = dapp_output.get("events")
    flat_config["dapp_calls"] = dapp_output.get("calls")
    flat_config["dapp_delegatecalls"] = dapp_output.get("delegatecalls")
    flat_config["dapp_zero_value_calls"] = dapp_output.get("zero_value_calls")
    flat_config["dapp_creations"] = dapp_output.get("creations")
    
    # Output settings for non-dapp
    non_dapp_output = output.get("non_dapp", {})
    flat_config["non_dapp_events"] = non_dapp_output.get("events")
    flat_config["non_dapp_calls"] = non_dapp_output.get("calls")
    flat_config["non_dapp_delegatecalls"] = non_dapp_output.get("delegatecalls")
    flat_config["non_dapp_zero_value_calls"] = non_dapp_output.get("zero_value_calls")
    flat_config["non_dapp_creations"] = non_dapp_output.get("creations")
    
    # Other output settings
    misc = nested_config.get("misc", {})
    flat_config["sensitive_events"] = misc.get("sensitive_events")
    flat_config["log_folder"] = misc.get("log_folder") 
    
    return flat_config

def validate_config(config):
    required_keys = [
        "port", "protocol", "host", "contracts_dapp", "contracts_non_dapp",
        "min_block", "max_block", "extract_normal_transactions", "extract_internal_transactions",
        "extract_transactions_by_events", "dapp_events", "dapp_calls", "dapp_zero_value_calls",
        "dapp_delegatecalls", "non_dapp_events", "non_dapp_calls", "non_dapp_zero_value_calls",
        "non_dapp_delegatecalls", "non_dapp_creations", "etherscan_api_key", "log_folder", "sensitive_events"
    ]
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required configuration key: {key}")
        if key == "etherscan_api_key" and config[key] == "ETHERSCAN_API_KEY":
            raise ValueError("Please provide your Etherscan API key in the configuration file")

def build_node_url(config):
    return f"{config['protocol']}{config['host']}:{config['port']}"

def initialize_extraction_state(config, utils_module):
    contracts_lx = list(map(utils_module.low, config["contracts_dapp"]))
    contracts_non_dapp = list(map(utils_module.low, config["contracts_non_dapp"]))
    return {
        "contracts_lx": set(contracts_lx),
        "base_contract": contracts_lx[0],
        "contracts_dapp": set(contracts_lx),
        "all_transactions": set(),
        "trace_tree": None,  
        "contracts_non_dapp": set(contracts_non_dapp)
    }
