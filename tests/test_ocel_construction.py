import json
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.trace_based_logging.log_construction import ocel_construction as oc

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
MAPPINGS = os.path.join(REPO, "src", "trace_based_logging", "log_construction",
                        "mappings.json")


def _addr(tag):
    """A well-formed, recognizable test address: 0x<tag><zero padding>."""
    return "0x" + tag.ljust(40, "0")


DEAD, ALICE = _addr("dead"), _addr("a11ce")
MAIN, REP, OTHER, MKT1 = _addr("75228d"), _addr("4e9"), _addr("07e4"), _addr("3a41")
ZERO = "0x" + "0" * 40


def object_model():
    return json.load(open(MAPPINGS))["object_model"]


def account_types():
    return pd.DataFrame({
        "address": [DEAD, ALICE, MAIN, REP, OTHER, MKT1],
        "account_type": ["EOA", "EOA", "CA", "CA", "CA", "CA"]})


def mini_ops():
    """Three transactions; tx 0xaaa is the W1 example
    (root call -> sub-call 1.2 -> log 1.2.1 + leaf call 1.2.2)."""
    rows = [
        # tx 0xaaa (block 100, txIndex 2) — the W1 example
        dict(txHash="0xaaa", tracePos=1, tracePosDepth="1", calltype="CALL",
             Activity="call without Ether transfer", frm=DEAD, to=MAIN),
        dict(txHash="0xaaa", tracePos=2, tracePosDepth="1.2", calltype="CALL",
             Activity="call to get balance", frm=MAIN, to=REP),
        dict(txHash="0xaaa", tracePos=3, tracePosDepth="1.2.1", calltype=None,
             Activity="minted tokens", address=REP, token=REP,
             tokenType_name="REP", target=ALICE),
        dict(txHash="0xaaa", tracePos=4, tracePosDepth="1.2.2", calltype="CALL",
             Activity="call without Ether transfer", frm=REP, to=OTHER),
        # tx 0xbbb (block 100, txIndex 5) — a direct deployment (CREATE at root)
        dict(txHash="0xbbb", tracePos=1, tracePosDepth="1", calltype="CREATE",
             Activity="create new contract", frm=DEAD, to=MKT1),
        # tx 0xccc (block 101, txIndex 0) — market lifecycle + roles
        dict(txHash="0xccc", tracePos=1, tracePosDepth="1", calltype="CALL",
             Activity="create market", frm=DEAD, to=MAIN, market=MKT1,
             marketType="yes-no", marketCreator=DEAD),
        dict(txHash="0xccc", tracePos=2, tracePosDepth="1.1", calltype=None,
             Activity="call to log initial report submission", address=MAIN,
             market=MKT1, reporter=ALICE),
        dict(txHash="0xccc", tracePos=3, tracePosDepth="1.2", calltype="CALL",
             Activity="contribute to dispute", frm=DEAD, to=MAIN, market=MKT1),
        dict(txHash="0xccc", tracePos=4, tracePosDepth="1.3", calltype="CALL",
             Activity="contribute to dispute", frm=DEAD, to=MAIN, market=MKT1),
        # order creation: the decoded 'creator' is an order role, not a CREATE
        dict(txHash="0xccc", tracePos=5, tracePosDepth="1.4", calltype="CALL",
             Activity="call to create order", frm=DEAD, to=MAIN,
             orderId="0xo1", orderType="bid", creator=ALICE),
    ]
    df = pd.DataFrame(rows).rename(columns={"frm": "from"})
    df["blockNumber"] = df["txHash"].map({"0xaaa": 100, "0xbbb": 100, "0xccc": 101})
    df["transactionIndex"] = df["txHash"].map({"0xaaa": 2, "0xbbb": 5, "0xccc": 0})
    df["timeStamp"] = df["blockNumber"].map({100: 1000, 101: 2000})
    # validation-only columns present in the input: must never reach events
    df["logIndex"] = 0
    df["orderExact"] = False
    return df


@pytest.fixture(scope="module")
def built():
    m, at, df = object_model(), account_types(), mini_ops()
    return {"A": oc.compose_ocel(df, m, variant="A", account_types=at),
            "B": oc.compose_ocel(df, m, variant="B", account_types=at),
            "df": df}


# ------------------------------- the mapping m -------------------------------

def test_augur_object_model_validates():
    oc.validate_object_model(object_model())


def test_object_model_errors_carry_paths():
    m = object_model()
    m["schema_version"] = 2
    with pytest.raises(oc.ObjectModelError, match="schema_version"):
        oc.validate_object_model(m)
    m = object_model()
    m["types"].append(dict(m["types"][0]))
    with pytest.raises(oc.ObjectModelError, match="duplicate type name"):
        oc.validate_object_model(m)
    m = object_model()
    m["null_values"] = ZERO
    with pytest.raises(oc.ObjectModelError, match="null_values"):
        oc.validate_object_model(m)


def test_null_values_of_the_mapping_become_no_object():
    """Augur writes the zero address into 'market' (and once into 'target') as its
    placeholder for 'none'; the mapping declares it null, so no MARKET or EOA object
    of the zero address exists, while the event keeps its other relations and the
    value as an attribute. The null value never enters the object universe, so it
    needs no account type."""
    df = mini_ops()
    transfer = dict(txHash="0xaaa", tracePos=5, tracePosDepth="1.2.3", calltype=None,
                    Activity="transferred tokens", address=REP, token=REP,
                    tokenType_name="REP", market=ZERO, target=ZERO, blockNumber=100,
                    transactionIndex=2, timeStamp=1000, logIndex=0, orderExact=False)
    df = pd.concat([df, pd.DataFrame([transfer])], ignore_index=True)
    m = object_model()
    with_rule = oc.compose_ocel(df, m, variant="B", account_types=account_types())
    assert not with_rule["objects"]["ocel:oid"].str.endswith(ZERO).any()
    assert not with_rule["e2o"]["ocel:oid"].str.endswith(ZERO).any()
    rel = with_rule["e2o"][with_rule["e2o"]["ocel:eid"] == "EID_0xaaa_5"]
    assert {"TOKEN", "CONTRACT", "TRANSACTION"} <= set(rel["ocel:type"])
    assert not {"MARKET", "EOA"} & set(rel["ocel:type"])
    event = with_rule["events"].set_index("ocel:eid").loc["EID_0xaaa_5"]
    assert event["market"] == ZERO and event["target"] == ZERO
    # the rule comes from the mapping: without the declaration both objects exist,
    # and the zero address then needs an account type like any other object
    m["null_values"] = []
    at = pd.concat([account_types(), pd.DataFrame({"address": [ZERO], "account_type": ["EOA"]})],
                   ignore_index=True)
    without = oc.compose_ocel(df, m, variant="B", account_types=at)
    zero_objects = without["objects"][without["objects"]["ocel:oid"].str.endswith(ZERO)]
    assert set(zero_objects["ocel:type"]) == {"MARKET", "EOA"}
    assert len(without["objects"]) - len(with_rule["objects"]) == 2
    assert len(without["e2o"]) - len(with_rule["e2o"]) == 2      # one MARKET, one EOA_TO
    for table in ("events", "o2o", "object_changes"):
        assert len(without[table]) == len(with_rule[table])


def test_null_values_do_not_filter_call_frames():
    """A call to the zero address is a fact of the trace, not a parameter: the
    CALLEE relation stays, and the address then needs an account type."""
    df = mini_ops()
    call = dict(txHash="0xaaa", tracePos=5, tracePosDepth="1.2.3", calltype="CALL",
                Activity="call without Ether transfer", blockNumber=100,
                transactionIndex=2, timeStamp=1000, logIndex=0, orderExact=False,
                **{"from": REP, "to": ZERO})
    df = pd.concat([df, pd.DataFrame([call])], ignore_index=True)
    with pytest.raises(ValueError, match="without account type"):
        oc.compose_ocel(df, object_model(), variant="B", account_types=account_types())
    at = pd.concat([account_types(), pd.DataFrame({"address": [ZERO], "account_type": ["EOA"]})],
                   ignore_index=True)
    built = oc.compose_ocel(df, object_model(), variant="B", account_types=at)
    rel = built["e2o"][(built["e2o"]["ocel:eid"] == "EID_0xaaa_5")
                       & (built["e2o"]["ocel:qualifier"] == "CALLEE")]
    assert rel["ocel:oid"].tolist() == ["EOA_" + ZERO]


# ------------------------------ events & ordering ------------------------------

def test_event_ids_unique_and_universe_complete(built):
    ev = built["A"]["events"]
    assert len(ev) == len(built["df"])
    assert ev["ocel:eid"].is_unique
    assert ev["ocel:eid"].str.match(r"EID_0x[a-f]+_\d+").all()


def test_sort_and_ordered_timestamp(built):
    ev = built["A"]["events"]
    # (blockNumber, transactionIndex, tracePos): 0xaaa (txi 2) before 0xbbb (txi 5)
    assert list(ev["txHash"].drop_duplicates()) == ["0xaaa", "0xbbb", "0xccc"]
    assert ev["timeStamp_ordered"].is_monotonic_increasing
    assert ev["timeStamp_ordered"].is_unique
    # time element stays the pure block timestamp
    assert set(ev["ocel:timestamp"].unique()) == \
        {pd.Timestamp(1000, unit="s"), pd.Timestamp(2000, unit="s")}


def test_forbidden_validation_columns_excluded(built):
    for variant in ("A", "B"):
        cols = built[variant]["events"].columns
        assert "logIndex" not in cols and "orderExact" not in cols


# ------------------------------- objects (A) -------------------------------

def test_objects_types_categories_account_type(built):
    ob = built["A"]["objects"].set_index("ocel:oid")
    assert ob.loc["TOKEN_" + REP, "category"] == "REP"
    assert ob.loc["MARKET_" + MKT1, "category"] == "yes-no"
    assert ob.loc["EOA_" + DEAD, "account_type"] == "EOA"
    # never-emitting callee and created contract become objects
    assert ob.loc["CONTRACT_" + OTHER, "account_type"] == "CA"
    assert "CONTRACT_" + MKT1 in ob.index          # via CREATED_CONTRACT
    # A carries no workaround objects
    assert not ob.index.str.startswith(("TX_", "FRAME_")).any()


# ------------------------------- relations (A) -------------------------------

def rel_set(d):
    return set(map(tuple, d["e2o"][["ocel:eid", "ocel:oid", "ocel:qualifier"]].values))


def test_call_frame_relations_and_single_roles(built):
    rels = rel_set(built["A"])
    assert ("EID_0xaaa_2", "CONTRACT_" + REP, "CALLEE") in rels
    assert ("EID_0xaaa_2", "EOA_" + DEAD, "CALLER") not in rels   # caller is MAIN
    assert ("EID_0xaaa_2", "CONTRACT_" + MAIN, "CALLER") in rels
    assert ("EID_0xbbb_1", "EOA_" + DEAD, "CREATOR") in rels
    assert ("EID_0xbbb_1", "CONTRACT_" + MKT1, "CREATED_CONTRACT") in rels
    # the six relations of the published log intact: emitter + token + target on the log event
    assert ("EID_0xaaa_3", "CONTRACT_" + REP, "CONTRACT") in rels
    assert ("EID_0xaaa_3", "TOKEN_" + REP, "TOKEN") in rels
    assert ("EID_0xaaa_3", "EOA_" + ALICE, "EOA_TO") in rels
    # exactly one CALLEE per call event (W2 discipline precondition)
    e2o = built["B"]["e2o"]
    callee_counts = e2o[e2o["ocel:qualifier"] == "CALLEE"].groupby("ocel:eid").size()
    assert (callee_counts == 1).all()


def test_variant_A_has_no_workaround_relations(built):
    quals = set(built["A"]["e2o"]["ocel:qualifier"])
    assert "contained-by" not in quals and "TRANSACTION" not in quals
    assert "MARKETCREATOR" not in quals and "REPORTER" not in quals


# ---------------------------- O2O + state history ----------------------------

def test_o2o_creates_both_directions(built):
    o2o = set(map(tuple, built["A"]["o2o"].values))
    assert ("EOA_" + DEAD, "creates", "CONTRACT_" + MKT1) in o2o
    assert ("CONTRACT_" + MKT1, "created by", "EOA_" + DEAD) in o2o


def test_market_state_history_with_dedupe(built):
    ch = built["A"]["object_changes"]
    mkt = ch[ch["ocel:oid"] == "MARKET_" + MKT1]
    assert list(mkt["state"]) == ["created", "reported", "disputed"]   # 2nd dispute deduped
    assert (mkt["ocel:field"] == "state").all()


# ------------------------------- variant B -------------------------------

def test_w1_matches_design_doc_example(built):
    b = built["B"]
    ob = set(b["objects"]["ocel:oid"])
    assert {"TX_0xaaa", "TX_0xbbb", "TX_0xccc", "FRAME_0xaaa_1.2"} <= ob
    assert not any(o.startswith("FRAME_") and o != "FRAME_0xaaa_1.2" for o in ob)
    rels = rel_set(b)
    # case membership: every event carries exactly one TRANSACTION tag to its TX
    tags = b["e2o"][b["e2o"]["ocel:qualifier"] == "TRANSACTION"]
    assert len(tags) == len(b["events"]) and tags["ocel:eid"].is_unique
    assert ("EID_0xaaa_1", "TX_0xaaa", "TRANSACTION") in rels
    assert ("EID_0xccc_4", "TX_0xccc", "TRANSACTION") in rels
    # contained-by only inside non-root frames; top-level = recognizable absence
    contained = {(e, o) for e, o, q in rels if q == "contained-by"}
    assert ("EID_0xaaa_3", "FRAME_0xaaa_1.2") in contained   # log in 1.2
    assert ("EID_0xaaa_4", "FRAME_0xaaa_1.2") in contained   # leaf call in 1.2
    assert not any(e in ("EID_0xaaa_1", "EID_0xaaa_2") for e, _ in contained)
    assert not any(o.startswith("TX_") for _, o in contained)
    cb = b["e2o"][b["e2o"]["ocel:qualifier"] == "contained-by"]
    assert len(cb) == 2 and cb["ocel:eid"].is_unique
    # O2O: TRANSACTION in no O2O; frame nesting only (none in this example —
    # FRAME_0xaaa_1.2's parent is the root)
    o2o = set(map(tuple, b["o2o"].values))
    assert sum(1 for a, q, c in o2o if q == "contains") == 0
    assert not any(a.startswith("TX_") or c.startswith("TX_") for a, _, c in o2o)


def test_w1_frame_nesting_contains():
    """A deeper tree: frame->frame contains edges exist, TX edges do not."""
    m, at = object_model(), account_types()
    rows = [
        dict(txHash="0xddd", tracePos=1, tracePosDepth="1", calltype="CALL",
             Activity="call without Ether transfer", frm=DEAD, to=MAIN),
        dict(txHash="0xddd", tracePos=2, tracePosDepth="1.2", calltype="CALL",
             Activity="call without Ether transfer", frm=MAIN, to=REP),
        dict(txHash="0xddd", tracePos=3, tracePosDepth="1.2.2", calltype="CALL",
             Activity="call without Ether transfer", frm=REP, to=OTHER),
        dict(txHash="0xddd", tracePos=4, tracePosDepth="1.2.2.1", calltype="CALL",
             Activity="call without Ether transfer", frm=OTHER, to=MAIN),
    ]
    df = pd.DataFrame(rows).rename(columns={"frm": "from"})
    df["blockNumber"], df["transactionIndex"], df["timeStamp"] = 100, 0, 1000
    b = oc.compose_ocel(df, m, variant="B", account_types=at)
    o2o = set(map(tuple, b["o2o"].values))
    assert ("FRAME_0xddd_1.2", "contains", "FRAME_0xddd_1.2.2") in o2o
    assert not any(a.startswith("TX_") for a, q, c in o2o if q == "contains")
    contained = {(e, o) for e, o, q in rel_set(b) if q == "contained-by"}
    assert ("EID_0xddd_3", "FRAME_0xddd_1.2") in contained
    assert ("EID_0xddd_4", "FRAME_0xddd_1.2.2") in contained
    assert ("EID_0xddd_1", "TX_0xddd", "TRANSACTION") in rel_set(b)


def test_w1_contains_skips_frames_outside_the_log():
    """Operations missing from the log (other category, non-DApp call) leave no
    frame object; a frame is contained by the nearest ancestor that IS one, and
    a frame with no such ancestor has no contains edge (top-level), as for events."""
    m, at = object_model(), account_types()
    rows = [
        dict(txHash="0xeee", tracePos=1, tracePosDepth="1", calltype="CALL",
             Activity="call without Ether transfer", frm=DEAD, to=MAIN),
        # 1.3 not logged; 1.3.1 logged; 1.3.1.2 not logged; 1.3.1.2.1 logged
        dict(txHash="0xeee", tracePos=3, tracePosDepth="1.3.1", calltype="CALL",
             Activity="call without Ether transfer", frm=MAIN, to=REP),
        dict(txHash="0xeee", tracePos=5, tracePosDepth="1.3.1.2.1", calltype="CALL",
             Activity="call without Ether transfer", frm=REP, to=OTHER),
        # 1.6 and 1.6.2 not logged; 1.6.2.1 logged: frame 1.6.2 has no logged ancestor
        dict(txHash="0xeee", tracePos=8, tracePosDepth="1.6.2.1", calltype="CALL",
             Activity="call without Ether transfer", frm=OTHER, to=MAIN),
    ]
    df = pd.DataFrame(rows).rename(columns={"frm": "from"})
    df["blockNumber"], df["transactionIndex"], df["timeStamp"] = 100, 0, 1000
    b = oc.compose_ocel(df, m, variant="B", account_types=at)
    frames = {o for o in b["objects"]["ocel:oid"] if o.startswith("FRAME_")}
    assert frames == {"FRAME_0xeee_1.3", "FRAME_0xeee_1.3.1.2", "FRAME_0xeee_1.6.2"}
    contains = {(a, c) for a, q, c in map(tuple, b["o2o"].values) if q == "contains"}
    assert contains == {("FRAME_0xeee_1.3", "FRAME_0xeee_1.3.1.2")}
    # every contains endpoint is an object: nothing for pm4py's writer to drop
    assert {x for pair in contains for x in pair} <= frames


def test_w2_identity_event_sets_equal(built):
    e2o = built["B"]["e2o"]
    token_events = set(e2o.loc[e2o["ocel:oid"] == "TOKEN_" + REP, "ocel:eid"])
    contract_events = set(e2o.loc[e2o["ocel:oid"] == "CONTRACT_" + REP, "ocel:eid"])
    # full equality spans ALL participation: callee (r2), emitter+param (r3),
    # and caller (r4 — REP makes the leaf call)
    assert token_events == contract_events == \
        {"EID_0xaaa_2", "EID_0xaaa_3", "EID_0xaaa_4"}
    # mirror carries the type-tag qualifier, not the role
    rels = rel_set(built["B"])
    assert ("EID_0xaaa_2", "TOKEN_" + REP, "TOKEN") in rels
    assert ("EID_0xaaa_2", "TOKEN_" + REP, "CALLEE") not in rels


def test_w2_market_contract_pair_mirrored(built):
    """The paper's own example class (a market is also a contract) is mirrored
    alongside TOKEN∩CONTRACT."""
    e2o = built["B"]["e2o"]
    market_events = set(e2o.loc[e2o["ocel:oid"] == "MARKET_" + MKT1, "ocel:eid"])
    contract_events = set(e2o.loc[e2o["ocel:oid"] == "CONTRACT_" + MKT1, "ocel:eid"])
    # creation (0xbbb) + the four market-param events (0xccc), on both twins
    assert market_events == contract_events == \
        {"EID_0xbbb_1", "EID_0xccc_1", "EID_0xccc_2", "EID_0xccc_3", "EID_0xccc_4"}
    rels = rel_set(built["B"])
    assert ("EID_0xccc_1", "CONTRACT_" + MKT1, "CONTRACT") in rels   # type-tag
    assert ("EID_0xbbb_1", "MARKET_" + MKT1, "MARKET") in rels       # type-tag
    # roles never mirrored: the creation keeps one CREATED_CONTRACT
    assert ("EID_0xbbb_1", "MARKET_" + MKT1, "CREATED_CONTRACT") not in rels


def test_w3_role_qualifiers(built):
    rels = rel_set(built["B"])
    assert ("EID_0xccc_1", "EOA_" + DEAD, "MARKETCREATOR") in rels
    assert ("EID_0xccc_2", "EOA_" + ALICE, "REPORTER") in rels
    # the order 'creator' role never reuses the creation-frame CREATOR
    assert ("EID_0xccc_5", "EOA_" + ALICE, "ORDER_CREATOR") in rels
    assert ("EID_0xccc_5", "EOA_" + ALICE, "CREATOR") not in rels


def test_role_qualifier_reserved_names_rejected():
    m = object_model()
    m["role_columns"]["creator"] = "CREATOR"
    with pytest.raises(oc.ObjectModelError, match="reserved"):
        oc.validate_object_model(m)


def test_b_superset_of_a(built):
    assert rel_set(built["A"]) <= rel_set(built["B"])
    assert set(built["A"]["objects"]["ocel:oid"]) <= set(built["B"]["objects"]["ocel:oid"])
    assert built["A"]["events"]["ocel:eid"].equals(built["B"]["events"]["ocel:eid"])


# ------------------------------- input contract -------------------------------

def test_engine_rejects_bad_input():
    m, at = object_model(), account_types()
    with pytest.raises(ValueError, match="core columns"):
        oc.compose_ocel(pd.DataFrame({"txHash": []}), m, account_types=at)
    df = mini_ops()
    df.loc[0, "transactionIndex"] = 1500
    with pytest.raises(ValueError, match="transactionIndex"):
        oc.compose_ocel(df, m, account_types=at)
    with pytest.raises(ValueError, match="account_types"):
        oc.compose_ocel(mini_ops(), m)
