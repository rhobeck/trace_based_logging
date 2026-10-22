import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.trace_based_logging.log_construction import log_construction_augur as lca
from tests.test_ocel_construction import (account_types, mini_ops,
                                          DEAD, ALICE, MAIN, REP, OTHER, MKT1)

BASE = "0x75228dce4d82566d93068a8d5d49435216551599"


def make_config():
    return {"base_contract": BASE, "min_block": 100, "max_block": 101,
            "log_folder": "output",
            "dapp_events": True, "dapp_calls": True,
            "dapp_delegatecalls": False, "dapp_zero_value_calls": False,
            "dapp_creations": True,
            "non_dapp_events": False, "non_dapp_calls": False,
            "non_dapp_delegatecalls": False, "non_dapp_zero_value_calls": False,
            "non_dapp_creations": False}


@pytest.fixture()
def project(tmp_path):
    """A tiny 'transformed' project: the mini fixture split into category tables
    under the established naming scheme, plus an account_types parquet."""
    resources = tmp_path / "resources"
    out = resources / "output" / "transformation"
    out.mkdir(parents=True)
    df = mini_ops()
    df["timeStamp"] = pd.to_datetime(df["timeStamp"], unit="s")
    events = df[df["calltype"].isna()]
    creations = df[df["calltype"] == "CREATE"]
    calls = df[df["calltype"] == "CALL"]
    suffix = f"{BASE}_100_101"
    events.to_parquet(out / f"dapp_events_{suffix}.parquet", index=False)
    calls.to_parquet(out / f"dapp_calls_{suffix}.parquet", index=False)
    creations.to_parquet(out / f"dapp_creations_{suffix}.parquet", index=False)
    # the run's trace tree (what address_classification reads for evidence)
    extraction = resources / "output" / "extraction"
    extraction.mkdir()
    tree = df[["calltype", "tracePosDepth", "from", "to", "address"]].rename(
        columns={"calltype": "type"})
    tree.to_parquet(extraction / f"df_trace_tree_{suffix}.parquet", index=False)
    at_path = tmp_path / "account_type.parquet"
    account_types().to_parquet(at_path, index=False)
    return str(resources), str(at_path), len(df)


def test_build_log_end_to_end_roundtrip(project):
    resources, at_path, n_ops = project
    results = lca.build_log(resources, "output", make_config(),
                            account_types_path=at_path)

    # the single paper log: full Algorithm 1 plus the Section-8 workarounds
    assert set(results) == {"paper"}
    assert results["paper"]["events"] == n_ops
    assert results["paper"]["object_changes"] == 3      # market state history

    out = os.path.join(resources, "output", "transformation")
    for table in lca.OCEL_TABLES:
        assert os.path.exists(os.path.join(out, "log_paper", f"{table}.parquet"))
    assert os.path.exists(os.path.join(out, f"ocel_{BASE}_100_101.sqlite"))
    xml_path = os.path.join(out, f"ocel_{BASE}_100_101.xmlocel")
    assert os.path.exists(xml_path)
    head = open(xml_path).read(2000)
    # OCEL 2.0 schema, not the 1.0 layout
    assert "<object-types>" in head
    assert '<global scope=' not in head

    # pm4py round-trip of the primary (SQLite) form
    import pm4py
    ocel = pm4py.read_ocel2(os.path.join(out, f"ocel_{BASE}_100_101.sqlite"))
    assert len(ocel.events) == n_ops
    assert len(ocel.objects) == results["paper"]["objects"]
    objects = pd.read_parquet(os.path.join(out, "log_paper", "objects.parquet"))
    # the workarounds are present in the paper log: W1 transaction objects
    assert objects["ocel:oid"].str.startswith("TX_").sum() == 3


def test_build_log_classifies_accounts_itself(project):
    """Without a recorded table, build_log types the run's own addresses
    (getCode stubbed) and checkpoints the table in the run's folder."""
    resources, _, n_ops = project
    code_at_max = {MAIN: 1, REP: 1, OTHER: 1}                  # MKT1: created
    queried = []

    def fake_get_code(address, block):
        queried.append((address, block))
        return code_at_max.get(address, 0)

    results = lca.build_log(resources, "output", make_config(),
                            account_types_path=None, export_xml=False,
                            get_code=fake_get_code)
    assert results["paper"]["events"] == n_ops
    out = os.path.join(resources, "output", "transformation")
    table = pd.read_parquet(os.path.join(out, f"account_types_{BASE}_100_101.parquet"))
    types = dict(zip(table["address"], table["account_type"]))
    # DEAD sends the sub-calls 1.2-1.4 of tx 0xccc in the fixture, which only
    # code can do: trace evidence makes it a CA despite no code at max_block
    assert types == {DEAD: "CA", ALICE: "EOA", MAIN: "CA",
                     REP: "CA", OTHER: "CA", MKT1: "CA"}
    assert set(table.loc[table["address"] == DEAD, "source"]) == {"trace_evidence"}
    assert set(table.loc[table["address"] == MKT1, "source"]) == {"creations"}
    assert all(block == 101 for _, block in queried)          # the run's max_block
    assert MKT1 not in dict(queried)                           # created: no query
    objects = pd.read_parquet(os.path.join(out, "log_paper", "objects.parquet"))
    assert objects.set_index("ocel:oid").loc["CONTRACT_" + MKT1, "account_type"] == "CA"


def test_account_types_reused_only_if_they_cover_all_addresses(project):
    """A checkpointed table is reused as-is when complete; one that misses an
    object address (e.g. after a mapping change) is classified again."""
    resources, _, _ = project
    queried = []

    def fake_get_code(address, block):
        queried.append(address)
        return 1 if address in (MAIN, REP, OTHER) else 0

    lca.build_log(resources, "output", make_config(), export_xml=False,
                  get_code=fake_get_code)
    path = os.path.join(resources, "output", "transformation",
                        f"account_types_{BASE}_100_101.parquet")
    complete = pd.read_parquet(path)

    queried.clear()
    lca.build_log(resources, "output", make_config(), export_xml=False,
                  get_code=fake_get_code)
    assert queried == []                                       # reused

    complete[complete["address"] != ALICE].to_parquet(path, index=False)
    lca.build_log(resources, "output", make_config(), export_xml=False,
                  get_code=fake_get_code)
    assert queried == []                                       # getCode cache
    pd.testing.assert_frame_equal(pd.read_parquet(path), complete)


def test_load_overall_dataframe_projection_honored_when_asked(project):
    resources, _, _ = project
    cfg = make_config()
    full = lca.load_overall_dataframe(resources, "output", ["dapp_events"], None, cfg)
    assert "token" in full.columns            # no projection by default path
    narrow = lca.load_overall_dataframe(resources, "output", ["dapp_events"],
                                        {"txHash", "Activity"}, cfg)
    assert set(narrow.columns) == {"txHash", "Activity"}
