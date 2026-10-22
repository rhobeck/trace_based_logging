from . import decoder
from src.trace_based_logging.logging_config import setup_logging

logger = setup_logging()


def build_topic_maps(dict_abi):
    """Precompute decoder.get_topic_map once per contract ABI.

    Invalid ABIs yield no entry — decoding then falls through to the fallback ABIs,
    matching the tolerance of the previous per-row path (where get_topic_map raised
    inside the loop and the next ABI was tried).
    """
    maps = {}
    for address, abi in dict_abi.items():
        try:
            maps[address] = decoder.get_topic_map(abi)
        except Exception:
            logger.debug(f"No topic map for {address} (invalid ABI); fallbacks will be used")
    return maps


def build_fallback_topic_maps(fallback_abis):
    """Precompute topic maps for the fallback ABIs (ERC-20/721/777/1155 standard
    events and the custom Augur events from config_custom_events.json), in their
    configured order — the cascade order is semantics and is preserved."""
    maps = []
    for name, abi in fallback_abis.items():
        try:
            maps.append((name, decoder.get_topic_map(abi)))
        except Exception:
            logger.debug(f"No topic map for fallback ABI {name}")
    return maps


def event_decoder(address, topics, data, dict_abi, fallback_abis,
                  topic_map_cache=None, fallback_topic_maps=None):
    """
    Decodes an event log using the contract's ABI first and a set of fallback ABIs
    (standard ERC events plus custom events) in configured order, via the vendored
    eth-event library (decoder.get_topic_map / decoder.decode_log).

    The optional topic_map_cache / fallback_topic_maps parameters accept maps
    precomputed by build_topic_maps / build_fallback_topic_maps; without them, the
    maps are built per call, which gives the same result and serves as the reference
    in the tests.

    Returns:
        dict: the decoded event (name, address, data) on success; None if the event
        cannot be decoded with any of the available ABIs.
    """
    dict_event = {"address": address, "topics": topics, "data": data}

    if topic_map_cache is not None and fallback_topic_maps is not None:
        candidates = []
        if address in topic_map_cache:
            candidates.append(("CONTRACT_ABI", topic_map_cache[address]))
        elif address not in dict_abi:
            logger.debug(f"No ABI available for {address}")
        candidates.extend(fallback_topic_maps)
        for key, topic_map in candidates:
            try:
                decoded_event = decoder.decode_log(dict_event, topic_map)
                logger.debug(f"Event decoded via {key}, name: {decoded_event['name']}, "
                             f"contract: {address}")
                return decoded_event
            except Exception:
                logger.debug(f"Decoding failed with given ABI: {key}, contract: {address}")
        return None

    # uncached path: maps built per call
    contract_abi = dict_abi.get(address, ["No ABI extracted for the contract"])
    if contract_abi == ["No ABI extracted for the contract"]:
        abis = fallback_abis
        logger.debug(f"No ABI available for {address}")
    else:
        abis = {"CONTRACT_ABI": contract_abi}
        abis.update(fallback_abis)

    for key, value in abis.items():
        try:
            topic_map = decoder.get_topic_map(value)
            decoded_event = decoder.decode_log(dict_event, topic_map)
            logger.debug(f"Event is in contract ABI {key}, decoded event name: "
                         f"{decoded_event['name']}, contract: {address}")
            return decoded_event
        except Exception:
            logger.debug(f"Decoding failed with given ABI: {key}, contract: {address}")
