from . import decoder
# import trace_decoder.decoder as decoder
import pandas as pd
from src.trace_based_logging.logging_config import setup_logging

logger = setup_logging()

def event_decoder(address, topics, data, dict_abi, fallback_abis):
    """
    Decodes an event log using a primary ABI and a set of fallback ABIs.

    This function attempts to decode an event log based on the provided ABIs. It starts with the primary ABI associated with the contract address and proceeds to try each of the fallback ABIs in order if decoding with the primary ABI fails.

    Args:
        address (str): The blockchain address from which the event originated. Used for logging purposes.
        topics (list of str): The list of topics associated with the event. The first topic is usually the hash of the event signature.
        data (str): The data payload of the event, containing the values of the event parameters.
        dict_abi (dict): A dictionary of all ABIs (Application Binary Interfaces) that was extracted for the contracts the events are associated with.
        fallback_abis (dict): A dictionary of fallback ABIs with their names as keys. These ABIs are used in order if the primary ABI fails to decode the event. In that case
            The fallback_abi contains: 
                # ERC-20
                # event Transfer(address indexed _from, address indexed _to, uint256 _value) 
                # NOTE: _from address set to 0x0 when tokens are created -> https://eips.ethereum.org/EIPS/eip-20#events
                # event Approval(address indexed _owner, address indexed _spender, uint256 _value)

                # ERC-777
                # event Sent(address indexed operator, address indexed from, address indexed to, uint256 amount, bytes data, bytes operatorData)
                # event Minted(address indexed operator, address indexed to, uint256 amount, bytes data, bytes operatorData)
                # event Burned(address indexed operator, address indexed from, uint256 amount, bytes data, bytes operatorData);
                # event AuthorizedOperator(address indexed operator, address indexed holder);
                # event RevokedOperator(address indexed operator, address indexed holder);

                # ERC-721
                # event Transfer(address indexed _from, address indexed _to, uint256 indexed _tokenId);
                # event Approval(address indexed _owner, address indexed _approved, uint256 indexed _tokenId);
                # event ApprovalForAll(address indexed _owner, address indexed _operator, bool _approved);

                # ERC-1155
                # event TransferSingle(address indexed _operator, address indexed _from, address indexed _to, uint256 _id, uint256 _value);
                # event TransferBatch(address indexed _operator, address indexed _from, address indexed _to, uint256[] _ids, uint256[] _values);
                # event ApprovalForAll(address indexed _owner, address indexed _operator, bool _approved);
                # event URI(string _value, uint256 indexed _id);

                # CUSTOM EVENTS, E.G., FOR AUGUR

    Returns:
        dict: A dictionary representing the decoded event if successful. The dictionary includes the event name and parameters.

    Raises:
        Exception: An exception is raised if the event cannot be decoded with any of the provided ABIs.
    """
    # if the address is not in a key in the dictionary, don't use the abi dictionary
    contract_abi = dict_abi.get(address, ["No ABI extracted for the contract"])
    if contract_abi == ["No ABI extracted for the contract"]:
        abis = fallback_abis
        logger.debug(f"No ABI available for {address}")
    else: 
        abis = {"CONTRACT_ABI": contract_abi}
        abis.update(fallback_abis)

    dict_event = {}
    dict_event = {"address": address, "topics": topics, "data": data}
    
    for key,value in abis.items():
        try:
            topic_map = decoder.get_topic_map(value)
            decoded_event = decoder.decode_log(dict_event, topic_map)
            logger.debug(f"Event is in contract ABI {key}, decoded event name: {decoded_event['name']}, contract: {address}")
            return decoded_event
        except Exception as e:  # Replace with specific exception if possible
            logger.debug(f"Decoding failed with given ABI: {key}, contract: {address}")
