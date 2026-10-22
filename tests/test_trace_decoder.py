import json
import os
import pickle
import socket
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.trace_based_logging.trace_decoder import data_preparation as dp
from src.trace_based_logging.trace_decoder import event_decoder

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
RES = os.path.join(REPO, "tests", "test_resources")
NODE_URL = "http://127.0.0.1:8081"

_cfg = json.load(open(os.path.join(REPO, "config.json")))
ETHERSCAN_KEY = _cfg.get("extraction", {}).get("etherscan_api_key", "")
key_available = ETHERSCAN_KEY not in ("", "ETHERSCAN_API_KEY")


def node_available():
    try:
        with socket.create_connection(("127.0.0.1", 8081), timeout=2):
            return True
    except OSError:
        return False


def load_fallback_abis():
    path = os.path.join(REPO, "src", "trace_based_logging", "trace_decoder",
                        "config_custom_events.json")
    return dp.load_event_definitions(path)


# ------------------------- create_abi_dict: cache & errors (mocked) -------------------------

class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload
    def json(self):
        return self._payload


def _fake_session_factory(responses_by_address, calls):
    class _FakeSession:
        def get(self, url, params=None, timeout=None):
            address = params["address"]
            calls.append(address)
            return _FakeResponse(responses_by_address[address])
    return _FakeSession


def test_abi_lookup_follows_the_non_dapp_switches():
    """ABIs are looked up only for addresses decoding uses: DApp contracts alone when
    every non-DApp category is off, all addresses as soon as one is on."""
    addresses = ["0xdapp1", "0xother", "0xdapp2"]
    contracts_dapp = {"0xDAPP1", "0xdapp2"}
    off = {flag: False for flag in dp.NON_DAPP_DECODING_FLAGS}
    off["non_dapp_creations"] = True          # creations use no ABIs
    assert dp.restrict_to_decoded_categories(addresses, contracts_dapp, off) == \
        ["0xdapp1", "0xdapp2"]
    for flag in dp.NON_DAPP_DECODING_FLAGS:
        on = dict(off, **{flag: True})
        assert dp.restrict_to_decoded_categories(addresses, contracts_dapp, on) == addresses


def test_create_abi_dict_caches_verified_and_not_verified(tmp_path, monkeypatch):
    abi = json.dumps([{"type": "event", "name": "E", "inputs": [], "anonymous": False}])
    responses = {
        "0xverified": {"status": "1", "message": "OK", "result": abi},
        "0xunverified": {"status": "0", "message": "NOTOK",
                         "result": "Contract source code not verified"},
    }
    calls = []
    monkeypatch.setattr(dp.requests, "Session", _fake_session_factory(responses, calls))
    cache = str(tmp_path / "abi_cache.parquet")

    d = dp.create_abi_dict(["0xverified", "0xunverified"], "KEY", cache_path=cache)
    assert list(d.keys()) == ["0xverified"]
    assert len(calls) == 2

    # second run: everything served from cache, zero HTTP
    d2 = dp.create_abi_dict(["0xverified", "0xunverified"], "KEY", cache_path=cache)
    assert list(d2.keys()) == ["0xverified"]
    assert len(calls) == 2
    cached = pd.read_parquet(cache)
    assert set(cached["address"]) == {"0xverified", "0xunverified"}
    assert set(cached["verified"]) == {True, False}


def test_create_abi_dict_persistent_rate_limit_raises_not_parsed(tmp_path, monkeypatch):
    """Rate-limit responses are pacing feedback with their own generous retry budget;
    a PERSISTENT rate limit still fails fast and is never recorded as an answer."""
    responses = {"0xa": {"status": "0", "message": "NOTOK",
                         "result": "Max calls per sec rate limit reached (3/sec)"}}
    calls = []
    monkeypatch.setattr(dp.requests, "Session", _fake_session_factory(responses, calls))
    monkeypatch.setattr(dp.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError):
        dp.create_abi_dict(["0xa"], "KEY", cache_path=str(tmp_path / "c.parquet"),
                           requests_per_second=1000)
    assert len(calls) == dp.RATE_LIMIT_MAX_RETRIES + 1


def test_create_abi_dict_genuine_error_consumes_error_budget(tmp_path, monkeypatch):
    responses = {"0xa": {"status": "0", "message": "NOTOK",
                         "result": "Missing/Invalid API Key"}}
    calls = []
    monkeypatch.setattr(dp.requests, "Session", _fake_session_factory(responses, calls))
    monkeypatch.setattr(dp.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError):
        dp.create_abi_dict(["0xa"], "KEY", cache_path=str(tmp_path / "c.parquet"),
                           requests_per_second=1000)
    assert len(calls) == dp.MAX_API_RETRIES


def test_create_abi_dict_pickle_short_circuit(tmp_path):
    stored = {"0xaddr": [{"type": "event"}]}
    abi_path = str(tmp_path / "dict_abi.pkl")
    pickle.dump(stored, open(abi_path, "wb"))
    assert dp.create_abi_dict(["0xother"], "KEY", abi_path=abi_path) == stored


# ------------------------------- base_transformation (UTC) -------------------------------

def test_base_transformation_timestamps_are_utc():
    df = pd.DataFrame({
        "from": ["0xA"], "to": ["0xB"], "address": [np.nan],
        "timeStamp": [0], "type": ["CALL"], "value": ["0x0"],
        "gas": ["0x1"], "gasUsed": ["0x1"],
    })
    out = dp.base_transformation(df, set())
    # unix 0 is 1970-01-01T00:00 UTC regardless of the machine's time zone
    assert out["timeStamp"].iloc[0] == pd.Timestamp("1970-01-01 00:00:00")
    assert out["calltype"].iloc[0] == "CALL"
    assert out["callvalue"].iloc[0] == 0    # hex converted


# ------------------------------- convert_hex_to_int (ported) -------------------------------

def test_convert_hex_to_int_valid_hexadecimal_values():
    df = pd.DataFrame({'gas': ['0x4A817C800', '0x6B49', '0xABC'],
                       'gasUsed': ['0x5208', '0x6BCD', '0x7F2']})
    expected = pd.DataFrame({'gas': [20000000000, 27465, 2748],
                             'gasUsed': [21000, 27597, 2034]})
    pd.testing.assert_frame_equal(dp.convert_hex_to_int(df), expected)


def test_convert_hex_to_int_invalid_and_missing():
    df = pd.DataFrame({'gas': ['0x4A817C800', '0xXYZ', None]})
    out = dp.convert_hex_to_int(df)
    assert out['gas'][0] == 20000000000
    assert out['gas'][1] == '0xXYZ'          # retained on error
    assert pd.isna(out['gas'][2])


def test_convert_hex_to_int_non_existent_column():
    df = pd.DataFrame({'gas': ['0xABC']})
    out = dp.convert_hex_to_int(df, list_of_cols=['gas', 'not_there'])
    assert out['gas'][0] == 2748


# ------------------------- decode_events golden (ported) + equivalence -------------------------

def _golden_event_df():
    base_contract = "0xbcc9946143534e28c3bad116cea0f81b9b208799"
    dict_abi = pickle.load(open(os.path.join(RES, f"dict_abi_{base_contract}_clean.pkl"), "rb"))
    df_log = pickle.load(open(os.path.join(RES, f"df_log_{base_contract}.pkl"), "rb"))
    # format drift fixes, as in the original test
    df_log["tracePos"] = df_log["order"]
    df_log["tracePosDepth"] = df_log["order"]
    df_log["address"] = df_log["address"].apply(
        lambda x: str(x).lower() if str(x) != "nan" else x)
    df_log["transactionIndex"] = None
    return df_log, dict_abi


def test_decode_events_golden():
    df_log, dict_abi = _golden_event_df()
    df_events = dp.decode_events(df_log.copy(), dict_abi)
    df_events.reset_index(drop=True, inplace=True)
    assert df_events["name"][1] == "Approval"        # fallback / standard event
    assert df_events["name"][33] == "Deposited"      # contract-specific ABI event
    # one output row per non-null-address input row (41 in this fixture). The original
    # test asserted `len(df_events == 40)` — tautologically true for any non-empty
    # frame (Tier-3 finding), so the literal 40 was never actually verified.
    assert len(df_events) == df_log["address"].notna().sum() == 41


def test_event_decoder_cached_equals_uncached_on_golden_rows():
    df_log, dict_abi = _golden_event_df()
    fallback_abis = load_fallback_abis()
    cache = event_decoder.build_topic_maps(dict_abi)
    fb_maps = event_decoder.build_fallback_topic_maps(fallback_abis)
    rows = df_log[df_log["address"].notna()].head(40)
    for _, row in rows.iterrows():
        uncached = event_decoder.event_decoder(
            row["address"], row["topics"], row["data"], dict_abi, fallback_abis)
        cached = event_decoder.event_decoder(
            row["address"], row["topics"], row["data"], dict_abi, fallback_abis,
            topic_map_cache=cache, fallback_topic_maps=fb_maps)
        assert uncached == cached


def test_event_decoder_invalid_abi_falls_back_to_standard():
    """Original 'rhubarb' assertion: an unusable contract ABI must fall through to the
    ERC-20 standard events — on both the cached and uncached paths."""
    fallback_abis = load_fallback_abis()
    tx = "0x9e8f42799dffe9f5700e1f871a20a9483e1c84db73c382c31a4412b9a2f83b2b"
    df_log = pickle.load(open(os.path.join(RES, f"df_log_tx_ERC_20_Transfer_{tx}.pkl"), "rb"))
    address = df_log["address"].iloc[1]
    topics = df_log["topics"].iloc[1]
    data = df_log["data"].iloc[1]
    dict_abi = {address: "rhubarb"}
    assert event_decoder.event_decoder(address, topics, data, dict_abi,
                                       fallback_abis)["name"] == "Transfer"
    cache = event_decoder.build_topic_maps(dict_abi)
    fb_maps = event_decoder.build_fallback_topic_maps(fallback_abis)
    assert event_decoder.event_decoder(address, topics, data, dict_abi, fallback_abis,
                                       topic_map_cache=cache,
                                       fallback_topic_maps=fb_maps)["name"] == "Transfer"


# ------------------------------- live Etherscan (key-gated) -------------------------------

@pytest.mark.skipif(not key_available, reason="Etherscan key not configured")
def test_abi_retrieval_live(tmp_path):
    addresses = pickle.load(open(os.path.join(RES, "addresses_6.pkl"), "rb"))
    cache = str(tmp_path / "abi_cache.parquet")
    dict_abi = dp.create_abi_dict(addresses, ETHERSCAN_KEY, cache_path=cache)
    assert len(dict_abi) == 5                         # original assertion: 5 of 6 verified
    # all six answers cached, including the negative one
    assert len(pd.read_parquet(cache)) == len(set(a.lower() for a in addresses))


@pytest.mark.skipif(not key_available, reason="Etherscan key not configured")
def test_event_decoder_live_erc_standards(tmp_path):
    fallback_abis = load_fallback_abis()
    cache = str(tmp_path / "abi_cache.parquet")
    expectations = [
        ("0xc4f4145f215d491be7123beacffe51d3d007a8060aab92826946c0dc744a9349",
         ["Transfer", "TokensMinted", "TokenBalanceChanged", "Transfer", "TokensTransferred"]),
        ("0x822ddf5837aee5cb46de3c2c62c0590f6bd81f11e46b1ec96e572e66fb338d25",
         ["Approval"]),
    ]
    for tx, names in expectations:
        df_log = pickle.load(open(os.path.join(RES, f"df_log_tx_ERC_20_Transfer_{tx}.pkl"), "rb"))
        for i, expected_name in enumerate(names):
            address = df_log["address"].iloc[i]
            dict_abi = dp.create_abi_dict([address], ETHERSCAN_KEY, cache_path=cache)
            decoded = event_decoder.event_decoder(
                address, df_log["topics"].iloc[i], df_log["data"].iloc[i],
                dict_abi, fallback_abis)
            assert decoded["name"] == expected_name


# ------------------------------- live node (function decoding) -------------------------------

def _golden_function_df():
    df_log = pickle.load(open(os.path.join(
        RES, "df_trace_tree_0x75228dce4d82566d93068a8d5d49435216551599_5937093_7000011.pkl"), "rb"))
    contracts_dapp = pickle.load(open(os.path.join(
        RES, "contracts_dapp_0x75228dce4d82566d93068a8d5d49435216551599_5926229_11229573.pkl"), "rb"))
    # original test's format-drift fixes (order encoded in address suffixes)
    df_log["order_calls"] = df_log["from"].apply(lambda x: int(x[43:]) if isinstance(x, str) else 0)
    df_log["order_events"] = df_log["address"].apply(lambda x: int(x[43:]) if isinstance(x, str) else 0)
    df_log["tracePos"] = df_log["order_calls"] + df_log["order_events"]
    df_log["tracePosDepth"] = df_log["tracePos"]
    df_log["transactionIndex"] = None
    df_log.drop(["order_calls", "order_events"], axis=1, inplace=True)
    df_log["from"] = df_log["from"].apply(lambda x: x[:42] if isinstance(x, str) else np.nan)
    df_log["address"] = df_log["address"].apply(lambda x: x[:42] if isinstance(x, str) else np.nan)
    df_log = dp.base_transformation(df_log, contracts_dapp)
    dict_abi = pickle.load(open(os.path.join(
        RES, "dict_abi_0x75228dce4d82566d93068a8d5d49435216551599_5926229_11229573.pkl"), "rb"))
    return df_log, dict_abi


@pytest.mark.skipif(not node_available(), reason="archive node at 127.0.0.1:8081 not reachable")
def test_process_abi_golden():
    df_log, dict_abi = _golden_function_df()
    contract_address = df_log["to"][722]
    contract = dp.process_abi(dict_abi[contract_address], contract_address, NODE_URL)
    func_obj, func_params = contract.decode_function_input(df_log["input"][722])
    assert str(func_obj) == ("<Function publicTradeWithLimit(uint8,address,uint256,"
                             "uint256,uint256,bytes32,bytes32,bytes32,uint256)>")


@pytest.mark.skipif(not node_available(), reason="archive node at 127.0.0.1:8081 not reachable")
def test_function_decoder_golden():
    df_log, dict_abi = _golden_function_df()
    df_functions = dp.decode_functions(df_log, dict_abi, NODE_URL, ["CALL"], False,
                                       "DAPP WITH ETHER TRANSFER")
    assert df_functions["name"][4] == ("<Function publicTradeWithLimit(uint8,address,"
                                       "uint256,uint256,uint256,bytes32,bytes32,bytes32,uint256)>")
    assert len(df_functions) == 6
    # shared contract objects must give identical results
    contract_objects, failed = dp.build_contract_objects(dict_abi, NODE_URL)
    assert not failed
    df_shared = dp.decode_functions(df_log, dict_abi, NODE_URL, ["CALL"], False,
                                    "DAPP WITH ETHER TRANSFER",
                                    contract_objects=contract_objects)
    pd.testing.assert_frame_equal(df_functions, df_shared)


# ------------------------------ category split (no node needed) ------------------------------

def test_delegatecall_category_ignores_the_inherited_value():
    """DELEGATECALLs propagate the value of the parent scope (Section 7); the
    category holds every DELEGATECALL, whatever value the client reports."""
    df_log = pd.DataFrame([
        {"calltype": "DELEGATECALL", "callvalue": "0x0", "to": "0xaaaa", "input": "0x", "hash": "0x1"},
        {"calltype": "DELEGATECALL", "callvalue": "0x1", "to": "0xaaaa", "input": "0x", "hash": "0x2"},
        {"calltype": "CALL", "callvalue": "0x0", "to": "0xaaaa", "input": "0x", "hash": "0x3"},
    ])
    # no ABI known: every selected row comes back undecoded, which is all the selection test needs
    decoded = dp.decode_functions(df_log.copy(), {}, NODE_URL, ["DELEGATECALL"], None, "DELEGATECALLs",
                                  contract_objects={})
    assert sorted(decoded["hash"]) == ["0x1", "0x2"]
    without_ether = dp.decode_functions(df_log.copy(), {}, NODE_URL, ["DELEGATECALL"], True, "x",
                                        contract_objects={})
    assert list(without_ether["hash"]) == ["0x1"]
    with_ether = dp.decode_functions(df_log.copy(), {}, NODE_URL, ["DELEGATECALL"], False, "x",
                                     contract_objects={})
    assert list(with_ether["hash"]) == ["0x2"]
