import os
import socket
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.trace_based_logging.log_construction import address_classification as ac
from tests.test_ocel_construction import object_model, mini_ops

NODE_URL = "http://127.0.0.1:8081"
REP = "0x1985365e9f78359a9b6ad760e32412f4a445e862"          # REP token contract
DEPLOYER = "0x57f1c2953630056aaadb9bfbda05369e6af7872b"     # Augur deployer EOA (paper)


def node_available():
    try:
        with socket.create_connection(("127.0.0.1", 8081), timeout=2):
            return True
    except OSError:
        return False


A = {k: "0x" + c * 40 for k, c in zip("abcdefg", "abcdef1")}   # well-formed test addresses


def ops():
    """Operations: a creates f (CREATE); b calls c; d is a 'reporter' parameter;
    g is a sub-call sender that self-destructed (no code at max_block)."""
    return pd.DataFrame([
        dict(calltype="CREATE", tracePosDepth="1", **{"from": A["a"], "to": A["f"]}),
        dict(calltype="CALL", tracePosDepth="1", **{"from": A["b"], "to": A["c"]}),
        dict(calltype="CALL", tracePosDepth="1.1", **{"from": A["g"], "to": A["c"]},
             reporter=A["d"]),
        dict(calltype=None, tracePosDepth="1.2", address=A["e"], **{"from": None, "to": None}),
    ])


def trace_tree(df):
    return df.rename(columns={"calltype": "type"})[["type", "tracePosDepth", "from", "to", "address"]]


def test_universe_comes_from_the_mapping():
    df = ops()
    assert ac.collect_addresses(df, object_model()) == set(A.values())
    # a mapping without an 'address'-typed source: frames + the reporter role only,
    # so the log emitter e is no object and is not classified
    narrow = {"types": [{"name": "X", "id_prefix": "X_", "address": True,
                         "sources": [{"column": "reporter", "qualifier": "R"}]}],
              "role_columns": {}}
    assert ac.collect_addresses(df, narrow) == set(A.values()) - {A["e"]}


def test_three_sources_in_order():
    df = ops()
    code = {A["c"]: 123}                       # only c has code at max_block
    queried = []

    def get_code(address, block):
        queried.append(address)
        return code.get(address, 0)

    table = ac.classify_accounts(df, object_model(), trace_tree(df), 500, get_code)
    row = table.set_index("address")
    assert row.loc[A["f"], ["account_type", "source"]].tolist() == ["CA", "creations"]
    assert row.loc[A["c"], ["account_type", "source"]].tolist() == ["CA", "getCode"]
    assert row.loc[A["g"], ["account_type", "source"]].tolist() == ["CA", "trace_evidence"]
    assert row.loc[A["e"], ["account_type", "source"]].tolist() == ["CA", "trace_evidence"]
    for eoa in ("a", "b", "d"):
        assert row.loc[A[eoa], ["account_type", "source"]].tolist() == ["EOA", "getCode"]
    assert A["f"] not in queried               # created addresses are never queried
    assert set(table["address"]) == set(A.values())


def test_call_targets_are_not_evidence():
    df = ops()
    evidence = ac.contract_evidence(trace_tree(df))
    assert evidence == {A["g"], A["e"]}        # non-root sender, log emitter
    assert A["c"] not in evidence              # callee only


def test_cache_is_reused_only_for_the_same_block(tmp_path):
    cache = str(tmp_path / "getcode_cache.parquet")
    calls = []

    def get_code(address, block):
        calls.append(address)
        return 0

    ac.fetch_code_sizes([A["a"]], get_code, 7, cache_path=cache)
    ac.fetch_code_sizes([A["a"]], get_code, 7, cache_path=cache)
    assert calls == [A["a"]]                   # second call served from cache
    ac.fetch_code_sizes([A["a"]], get_code, 8, cache_path=cache)
    assert calls == [A["a"], A["a"]]           # different block: queried again


def test_engine_rejects_unknown_addresses():
    from src.trace_based_logging.log_construction import ocel_construction as oc
    from tests.test_ocel_construction import account_types
    partial = account_types().iloc[1:]         # drop the first address
    with pytest.raises(ValueError, match="without account type"):
        oc.compose_ocel(mini_ops(), object_model(), variant="paper", account_types=partial)


def test_created_comes_from_the_trace_tree():
    """A creation outside the logged operations (e.g. a category switched off)
    still makes its target a CA."""
    df = ops()
    logged = df[df["calltype"] != "CREATE"]                 # creation not logged
    logged = pd.concat([logged, pd.DataFrame([dict(calltype="CALL", tracePosDepth="1",
                                                   **{"from": A["b"], "to": A["f"]})])])
    table = ac.classify_accounts(logged, object_model(), trace_tree(df), 500,
                                 lambda address, block: 0)
    row = table.set_index("address")
    assert row.loc[A["f"], ["account_type", "source"]].tolist() == ["CA", "creations"]


def test_frame_fields_count_only_on_call_and_creation_rows():
    """As in the engine: 'from'/'to' of other rows (log entries) are no objects,
    so they are neither collected nor checked."""
    df = ops()
    df.loc[3, ["from", "to"]] = ["nan", A["a"].replace("a", "9")]
    assert ac.collect_addresses(df, object_model()) == set(A.values())


def test_null_values_are_not_collected_from_mapping_columns():
    """The zero address as a parameter value (Augur's placeholder for 'no market')
    becomes no object, so it is not classified; as a call target it is collected,
    as in the engine."""
    df = ops()
    df.loc[2, "reporter"] = ac.ZERO_ADDRESS
    assert ac.collect_addresses(df, object_model()) == set(A.values()) - {A["d"]}
    df.loc[1, "to"] = ac.ZERO_ADDRESS          # c stays: it is the target of row 2 too
    assert ac.collect_addresses(df, object_model()) == set(A.values()) - {A["d"]} | {ac.ZERO_ADDRESS}


def test_malformed_object_value_fails_before_any_query():
    df = ops()
    df.loc[2, "reporter"] = "nan"
    queried = []
    with pytest.raises(ValueError, match="'reporter'.*not addresses"):
        ac.classify_accounts(df, object_model(), trace_tree(df), 500,
                             lambda address, block: queried.append(address) or 0)
    assert queried == []


def test_failed_creation_does_not_create_the_zero_address():
    """A CREATE that runs out of gas reports the zero address as its result; the
    zero address is no contract, and typed EOA where it is an object."""
    df = ops()
    failed = pd.DataFrame([dict(calltype="CREATE", tracePosDepth="1", address=None,
                                **{"from": A["a"], "to": ac.ZERO_ADDRESS})])
    tree = pd.concat([trace_tree(df), trace_tree(failed)], ignore_index=True)
    assert ac.ZERO_ADDRESS not in ac.created_addresses(tree)
    logged = pd.concat([df, pd.DataFrame([dict(calltype="CALL", tracePosDepth="1.3",
                                               **{"from": A["c"], "to": ac.ZERO_ADDRESS})])],
                       ignore_index=True)
    table = ac.classify_accounts(logged, object_model(), tree, 500, lambda address, block: 0)
    row = table.set_index("address")
    assert row.loc[ac.ZERO_ADDRESS, ["account_type", "source"]].tolist() == ["EOA", "getCode"]


@pytest.mark.skipif(not node_available(), reason="archive node at 127.0.0.1:8081 not reachable")
def test_get_code_size_on_chain():
    """The real eth_getCode call: a contract has code, an EOA has none."""
    assert ac.get_code_size(REP, 11229573, NODE_URL) > 0
    assert ac.get_code_size(DEPLOYER, 11229573, NODE_URL) == 0
