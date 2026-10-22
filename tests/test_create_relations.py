import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.trace_based_logging.raw_trace_retriever import create_relations as cr

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
CREATIONS = os.path.join(REPO, "data", "creations_cache", "creations_dapp.parquet")
# fall back to the ZIP-dir copy if the repo data isn't populated
CREATIONS_ALT = "/Users/rhob/Documents/trace_based_logging-main/data/creations_dapp.parquet"
ROOTS = {"0x75228dce4d82566d93068a8d5d49435216551599",
         "0x57f1c2953630056aaadb9bfbda05369e6af7872b"}


def creations_path():
    for p in (CREATIONS, CREATIONS_ALT):
        if os.path.exists(p):
            return p
    return None


def test_forward_and_backward_closure():
    # A creates B; C creates A (backward); B creates D. Seed = {A}.
    df = pd.DataFrame([
        {"type": "CREATE", "from": "0xa", "to": "0xb"},
        {"type": "CREATE", "from": "0xc", "to": "0xa"},
        {"type": "CREATE", "from": "0xb", "to": "0xd"},
        {"type": "CREATE", "from": "0xz", "to": "0xy"},   # unrelated branch
    ])
    dapp = {"0xa"}
    result, new = cr.create_relations(df, dapp.copy(), dapp.copy(), set())
    assert result == {"0xa", "0xb", "0xc", "0xd"}           # forward B,D; backward C
    assert "0xy" not in result and "0xz" not in result       # unrelated branch excluded
    assert new == {"0xb", "0xc", "0xd"}                       # newly discovered this level


def test_create2_included_and_casing_normalized():
    df = pd.DataFrame([
        {"type": "CREATE2", "from": "0xA", "to": "0xB"},
        {"type": "CALL", "from": "0xA", "to": "0xQ"},        # non-CREATE ignored
    ])
    result, new = cr.create_relations(df, {"0xa"}, {"0xa"}, set())
    assert "0xb" in result           # CREATE2 followed, address lowercased
    assert "0xq" not in result       # CALL not a creation relation


def test_remove_predefined_contracts():
    # ported from the original test_pytest.py expectation
    contracts = ["0x123...", "0x456...", "0x0000000000000000000000000000000000000000", "0x789..."]
    non_dapp = ["0x0000000000000000000000000000000000000000", "0xabc...", "0xdef..."]
    assert cr.remove_contracts_non_dapp(contracts, non_dapp) == {"0x123...", "0x789...", "0x456..."}


def test_non_dapp_pruned_from_discovery():
    df = pd.DataFrame([
        {"type": "CREATE", "from": "0xa", "to": "0xbad"},
        {"type": "CREATE", "from": "0xa", "to": "0xgood"},
    ])
    result, new = cr.create_relations(df, {"0xa"}, {"0xa"}, {"0xbad"})
    assert "0xbad" not in result and "0xbad" not in new
    assert "0xgood" in result


def test_deterministic_and_idempotent():
    df = pd.DataFrame([
        {"type": "CREATE", "from": "0xa", "to": "0xb"},
        {"type": "CREATE", "from": "0xb", "to": "0xc"},
    ])
    r1, _ = cr.create_relations(df, {"0xa"}, {"0xa"}, set())
    r2, _ = cr.create_relations(df, {"0xa"}, {"0xa"}, set())
    assert r1 == r2 == {"0xa", "0xb", "0xc"}
    # re-running with the full set as input adds nothing
    r3, new3 = cr.create_relations(df, r1.copy(), set(), set())
    assert r3 == r1 and new3 == set()


@pytest.mark.skipif(creations_path() is None, reason="creations parquet not available")
def test_real_closure_reproduces_dapp_address_count():
    """Feeding the full creations table with the two seed roots must reproduce the
    paper's 21,096-address DApp set via forward+backward closure."""
    cre = pd.read_parquet(creations_path(), columns=["from", "to", "calltype"])
    df = cre.rename(columns={"calltype": "type"})[["type", "from", "to"]]
    result, _ = cr.create_relations(df, set(ROOTS), set(ROOTS),
                                    {"0x0000000000000000000000000000000000000000"})
    assert len(result) == 21096
    assert ROOTS.issubset(result)
    assert set(cre["to"].str.lower()).issubset(result)
