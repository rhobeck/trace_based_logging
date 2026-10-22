"""OCEL 2.0 construction engine — Algorithm 1 of the paper, with the mapping *m* as
an explicit parameter (the algorithm's own signature: "Require: … and mapping m").

The engine is DApp-agnostic: everything domain-specific — which parameter columns
become which object types, qualifiers, category sources, state maps, role columns,
which values are null placeholders — comes from the ``object_model`` section of a
mappings file (for Augur: ``mappings.json``). Everything DApp-independent — event identity and ordering,
Table 3's call-frame relations (CALLER/CALLEE/CREATOR/CREATED_CONTRACT), the
creates/created-by O2O, account typing, and the mechanics of the three Section-8
workarounds — is built in.

Variants: A builds the log of Algorithm 1 as described in Section 7; B adds the
three Section-8 workarounds (W1 transaction and call-frame objects, W2 mirrored
relations for multi-typed addresses, W3 role qualifiers); 'paper' is B written as
the single published log, whose file names carry no variant letter. A and B
remain for the tests.

Input contract: one wide DataFrame, one row per operation, with core columns
``txHash, tracePos, tracePosDepth, Activity, timeStamp, blockNumber,
transactionIndex, calltype, from, to`` plus any decoded parameter columns; and an
``account_types`` table (address → "EOA"/"CA", from address_classification) that
covers every address the log turns into an object — an unknown address is an error,
never silently an EOA.

Output: dict of pandas DataFrames — events, objects, e2o, o2o, object_changes —
ready for pm4py's OCEL object / ``write_ocel2`` (the export happens in the
orchestrator).
"""

import pandas as pd

from src.trace_based_logging.logging_config import setup_logging

logger = setup_logging()

VARIANT_A = "A"
VARIANT_B = "B"
# the published log: everything the paper describes, Algorithm 1 plus the three
# Section-8 workarounds; constructed like VARIANT_B, named without a variant letter
VARIANT_PAPER = "paper"
WORKAROUND_VARIANTS = (VARIANT_B, VARIANT_PAPER)

CORE_COLUMNS = ["txHash", "tracePos", "tracePosDepth", "Activity", "timeStamp",
                "blockNumber", "transactionIndex", "calltype", "from", "to"]
CALL_TYPES = ("CALL", "DELEGATECALL")
CREATE_TYPES = ("CREATE", "CREATE2")
# columns of the validation ordering mode; never event attributes
FORBIDDEN_EVENT_ATTRIBUTES = ("logIndex", "orderExact")

E2O_COLUMNS = ["ocel:eid", "ocel:oid", "ocel:type", "ocel:qualifier"]


class ObjectModelError(ValueError):
    """Raised when the mapping m is malformed: at load, with the path of the
    offending entry."""


# ------------------------------- validate m -------------------------------

def validate_object_model(object_model):
    def fail(path, msg):
        raise ObjectModelError(f"object_model{path}: {msg}")

    if not isinstance(object_model, dict):
        fail("", "must be a dict")
    if object_model.get("schema_version") != 1:
        fail("['schema_version']", "must be 1")
    types = object_model.get("types")
    if not isinstance(types, list) or not types:
        fail("['types']", "must be a non-empty list")
    names, prefixes = set(), set()
    for i, t in enumerate(types):
        path = f"['types'][{i}]"
        for key in ("name", "id_prefix", "sources"):
            if key not in t:
                fail(path, f"missing '{key}'")
        if t["name"] in names:
            fail(path, f"duplicate type name {t['name']!r}")
        if t["id_prefix"] in prefixes:
            fail(path, f"duplicate id_prefix {t['id_prefix']!r}")
        names.add(t["name"]); prefixes.add(t["id_prefix"])
        if not isinstance(t["sources"], list) or not t["sources"]:
            fail(path + "['sources']", "must be a non-empty list")
        for j, s in enumerate(t["sources"]):
            if not isinstance(s, dict) or "column" not in s or "qualifier" not in s:
                fail(path + f"['sources'][{j}]", "needs 'column' and 'qualifier'")
        if ("category_column" in t) == ("category_value" in t):
            fail(path, "exactly one of category_column/category_value required")
        if "state_map" in t:
            if not isinstance(t["state_map"], dict) or not all(
                    isinstance(k, str) and isinstance(v, str)
                    for k, v in t["state_map"].items()):
                fail(path + "['state_map']", "must map activity:str -> state:str")
    fot = object_model.get("frame_object_types")
    if not isinstance(fot, dict) or set(fot) != {"EOA", "CA"} \
            or not set(fot.values()) <= names:
        fail("['frame_object_types']",
             "must map 'EOA' and 'CA' to existing type names")
    roles = object_model.get("role_columns", {})
    if not isinstance(roles, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in roles.items()):
        fail("['role_columns']", "must map column:str -> qualifier:str")
    null_values = object_model.get("null_values", [])
    if not isinstance(null_values, list) or not all(isinstance(v, str) for v in null_values):
        fail("['null_values']", "must be a list of strings")
    reserved = {"CALLER", "CALLEE", "CREATOR", "CREATED_CONTRACT"} | names
    for col, qual in roles.items():
        if qual in reserved:
            fail(f"['role_columns'][{col!r}]", f"qualifier {qual!r} is reserved")
    w2 = object_model.get("w2_mirror_types", [])
    if w2:
        pairs = w2 if isinstance(w2[0], (list, tuple)) else [w2]
        seen = set()
        for k, p in enumerate(pairs):
            if len(p) != 2 or p[0] == p[1] or not set(p) <= names:
                fail(f"['w2_mirror_types'][{k}]",
                     "each pair must name two distinct existing types")
            key = frozenset(p)
            if key in seen:
                fail(f"['w2_mirror_types'][{k}]", f"duplicate pair {list(p)!r}")
            seen.add(key)


# --------------------------------- helpers ---------------------------------

def _lower(series):
    return series.dropna().astype(str).str.lower()


def _parent_depth(depth_series):
    """'1.2.3' -> '1.2'; '1' -> '' (root has no parent)."""
    return depth_series.astype(str).str.rpartition(".")[0]


def _type_cfg(object_model, name):
    return next(t for t in object_model["types"] if t["name"] == name)


def _w2_pairs(object_model):
    """w2_mirror_types, normalized to a list of pairs (a single flat pair is accepted too)."""
    w2 = object_model.get("w2_mirror_types", [])
    if not w2:
        return []
    return [list(p) for p in w2] if isinstance(w2[0], (list, tuple)) else [list(w2)]


def _null_values(object_model):
    """
    The values of the mapping m that identify no object.

    A DApp may fill an address parameter with a placeholder when the parameter does not
    apply. Augur logs every token with one event signature (TokensTransferred, TokensMinted,
    TokensBurned, all with a 'market' parameter) and writes the zero address into 'market'
    for tokens that belong to no market (REP, fee tokens); a mint to nobody names the zero
    address as its target in the same way. Algorithm 1 turns every parameter value into an
    object, so the placeholder would become a MARKET (and an EOA) with every REP transfer
    as its event. The zero address is no account: nobody holds its key and it never had
    code, so it is neither an EOA nor a CA, and the paper's account types know no third
    kind. Which values are null is domain knowledge, hence declared in the mapping
    (object_model['null_values']). The engine mints no object from such a value in the
    mapping's sources and role columns, so the value never enters the object universe and
    never needs an account type; the event keeps it as an attribute, where its meaning per
    column (no market, minted to nobody) stays readable. The call frame fields 'from' and
    'to' are not filtered: a call to an address is a fact of the trace, not a parameter.

    Args:
        object_model (dict): The mapping m.

    Returns:
        list: The lower-cased null values.
    """
    return [v.lower() for v in object_model.get("null_values", [])]


def _account_prefixes(object_model, account_type_map):
    """address -> id_prefix of its chain-truth type (frame-derived objects)."""
    eoa_cfg = _type_cfg(object_model, object_model["frame_object_types"]["EOA"])
    ca_cfg = _type_cfg(object_model, object_model["frame_object_types"]["CA"])
    return eoa_cfg, ca_cfg, account_type_map


# ------------------------------ the engine ------------------------------

def compose_ocel(df, object_model, variant=VARIANT_A, account_types=None):
    """Algorithm 1: compose the OCEL tables from the operation table and mapping m.

    Args:
        df: wide operation table (see module docstring; not mutated).
        object_model: the mapping m (validated at load).
        variant: VARIANT_A (Algorithm 1), VARIANT_B (adds W1 to W3) or
            VARIANT_PAPER (B with the published naming).
        account_types: DataFrame(address, account_type in {'EOA','CA'}).

    Returns:
        dict with keys events, objects, e2o, o2o, object_changes.
    """
    validate_object_model(object_model)
    if variant not in (VARIANT_A, VARIANT_B, VARIANT_PAPER):
        raise ValueError(f"variant must be A, B or paper, got {variant!r}")
    missing = [c for c in CORE_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"operation table lacks core columns: {missing}")
    if account_types is None:
        raise ValueError("account_types table is required (address_classification)")
    account_type_map = dict(zip(_lower(account_types["address"]),
                                account_types["account_type"]))
    null_values = _null_values(object_model)

    def account_type_of(addresses):
        acct = addresses.map(account_type_map)
        if acct.isna().any():
            unknown = sorted(set(addresses[acct.isna()]))
            raise ValueError(f"{len(unknown)} addresses without account type "
                             f"(e.g. {unknown[:3]}); the account_types table must "
                             "cover every object address")
        return acct

    # ---- event identity and ordering ----
    ts = df["timeStamp"]
    if not pd.api.types.is_datetime64_any_dtype(ts):
        ts = pd.to_datetime(pd.to_numeric(ts), unit="s")
    txi = pd.to_numeric(df["transactionIndex"]).astype("int64")
    tp = pd.to_numeric(df["tracePos"]).astype("int64")
    if not (txi < 1000).all():
        raise ValueError("transactionIndex >= 1000 breaks timeStamp_ordered offsets")
    if not (tp < 10 ** 6).all():
        raise ValueError("tracePos >= 1e6 breaks timeStamp_ordered offsets")
    eid = "EID_" + df["txHash"].astype(str) + "_" + tp.astype(str)
    if eid.duplicated().any():
        dupes = eid[eid.duplicated()].head(3).tolist()
        raise ValueError(f"event ids not unique, e.g. {dupes}")
    ordered_ts = ts + pd.to_timedelta(txi, unit="ms") + pd.to_timedelta(tp, unit="ns")

    order = pd.DataFrame({"blockNumber": pd.to_numeric(df["blockNumber"]),
                          "transactionIndex": txi, "tracePos": tp}).sort_values(
        ["blockNumber", "transactionIndex", "tracePos"]).index

    # ---- events table ----
    attr_cols = [c for c in df.columns
                 if c not in ("Activity", "timeStamp")
                 and c not in FORBIDDEN_EVENT_ATTRIBUTES]
    events = df[attr_cols].copy()
    events.insert(0, "ocel:eid", eid)
    events.insert(1, "ocel:activity", df["Activity"].astype(str))
    events.insert(2, "ocel:timestamp", ts)
    events["timeStamp_ordered"] = ordered_ts
    events = events.loc[order].reset_index(drop=True)

    eid_lookup = pd.DataFrame({"ocel:eid": eid})

    # ---- objects + config-driven e2o ----
    objects_frames, e2o_frames, changes_frames = [], [], []
    typed_addresses = {}          # type name -> set of raw ids (for W2)

    for t in object_model["types"]:
        ids_cat = []
        for s in t["sources"]:
            col = s["column"]
            if col not in df.columns:
                continue
            vals = _lower(df[col]) if t.get("address", False) \
                else df[col].dropna().astype(str)
            vals = vals[~vals.isin(null_values)]     # a null value identifies no object
            if vals.empty:
                continue
            part = pd.DataFrame({"id": vals})
            if "category_column" in t and t["category_column"] in df.columns:
                part["category"] = df.loc[vals.index, t["category_column"]]
            ids_cat.append(part)
            e2o_frames.append(pd.DataFrame({
                "ocel:eid": eid.loc[vals.index],
                "ocel:oid": t["id_prefix"] + vals,
                "ocel:type": t["name"],
                "ocel:qualifier": s["qualifier"]}))
        if not ids_cat:
            typed_addresses[t["name"]] = set()
            continue
        allv = pd.concat(ids_cat, ignore_index=True)
        if "category_column" in t:
            cat = allv.dropna(subset=["category"]).groupby("id")["category"].first() \
                if "category" in allv.columns else pd.Series(dtype=object)
            obj = pd.DataFrame({"id": sorted(set(allv["id"]))})
            obj["category"] = obj["id"].map(cat)
        else:
            obj = pd.DataFrame({"id": sorted(set(allv["id"]))})
            obj["category"] = t["category_value"]
        obj["ocel:oid"] = t["id_prefix"] + obj["id"]
        obj["ocel:type"] = t["name"]
        if t.get("address", False):
            obj["account_type"] = account_type_of(obj["id"])
        typed_addresses[t["name"]] = set(obj["id"])
        objects_frames.append(obj.drop(columns=["id"]))

        # evolving attribute history (Algorithm 1 ll. 9-16; state demonstration)
        if "state_map" in t:
            src = t["sources"][0]["column"]
            mask = df[src].notna() & df["Activity"].isin(t["state_map"])
            if mask.any():
                hist = pd.DataFrame({
                    "ocel:oid": t["id_prefix"] + _lower(df.loc[mask, src]),
                    "ocel:type": t["name"],
                    "ocel:timestamp": ts.loc[mask],
                    "_ordered": ordered_ts.loc[mask],
                    "ocel:field": "state",
                    "state": df.loc[mask, "Activity"].map(t["state_map"])})
                hist = hist.sort_values(["ocel:oid", "_ordered"])
                keep = hist["state"] != hist.groupby("ocel:oid")["state"].shift()
                changes_frames.append(hist[keep].drop(columns=["_ordered"]))

    # ---- built-in call-frame relations (Table 3 E2O cells) ----
    eoa_cfg, ca_cfg, _ = _account_prefixes(object_model, account_type_map)

    def frame_rel(mask, column, qualifier):
        vals = _lower(df.loc[mask, column])
        if vals.empty:
            return None
        acct = account_type_of(vals)
        cfgs = acct.map({"EOA": eoa_cfg["id_prefix"], "CA": ca_cfg["id_prefix"]})
        tnames = acct.map({"EOA": eoa_cfg["name"], "CA": ca_cfg["name"]})
        new_objects.append(pd.DataFrame({"id": vals, "acct": acct}))
        return pd.DataFrame({"ocel:eid": eid.loc[vals.index],
                             "ocel:oid": cfgs + vals, "ocel:type": tnames,
                             "ocel:qualifier": qualifier})

    new_objects = []
    calls_mask = df["calltype"].isin(CALL_TYPES)
    creates_mask = df["calltype"].isin(CREATE_TYPES)
    for mask, column, qualifier in [(calls_mask, "from", "CALLER"),
                                    (calls_mask, "to", "CALLEE"),
                                    (creates_mask, "from", "CREATOR"),
                                    (creates_mask, "to", "CREATED_CONTRACT")]:
        rel = frame_rel(mask, column, qualifier)
        if rel is not None:
            e2o_frames.append(rel)

    # frame-derived objects not already typed by the config
    if new_objects:
        nov = pd.concat(new_objects, ignore_index=True).drop_duplicates("id")
        for acct, cfg in (("EOA", eoa_cfg), ("CA", ca_cfg)):
            fresh = sorted(set(nov.loc[nov["acct"] == acct, "id"])
                           - typed_addresses.get(cfg["name"], set()))
            if fresh:
                add = pd.DataFrame({"ocel:oid": [cfg["id_prefix"] + a for a in fresh],
                                    "ocel:type": cfg["name"],
                                    "category": cfg.get("category_value"),
                                    "account_type": acct})
                objects_frames.append(add)
                typed_addresses[cfg["name"]] |= set(fresh)

    # ---- built-in O2O: creates / created by (Table 3's own sound example) ----
    cre = df.loc[creates_mask, ["from", "to"]]
    o2o_frames = []
    if not cre.empty:
        creator = _lower(cre["from"]); created = _lower(cre["to"])
        both = pd.DataFrame({"creator": creator, "created": created}).dropna()
        c_acct = account_type_of(both["creator"])
        creator_oid = c_acct.map({"EOA": eoa_cfg["id_prefix"],
                                  "CA": ca_cfg["id_prefix"]}) + both["creator"]
        created_oid = ca_cfg["id_prefix"] + both["created"]
        o2o_frames.append(pd.DataFrame({"ocel:oid": creator_oid,
                                        "ocel:qualifier": "creates",
                                        "ocel:oid_2": created_oid}))
        o2o_frames.append(pd.DataFrame({"ocel:oid": created_oid,
                                        "ocel:qualifier": "created by",
                                        "ocel:oid_2": creator_oid}))

    # ---- the Section-8 workarounds (variants B and paper) ----
    if variant in WORKAROUND_VARIANTS:
        # W3: trimmed EOA role relations, qualifier per role column
        for col, qualifier in object_model.get("role_columns", {}).items():
            if col not in df.columns:
                continue
            vals = _lower(df[col])
            vals = vals[~vals.isin(null_values)]     # a null value identifies no object
            if vals.empty:
                continue
            acct = account_type_of(vals)
            e2o_frames.append(pd.DataFrame({
                "ocel:eid": eid.loc[vals.index],
                "ocel:oid": acct.map({"EOA": eoa_cfg["id_prefix"],
                                      "CA": ca_cfg["id_prefix"]}) + vals,
                "ocel:type": acct.map({"EOA": eoa_cfg["name"], "CA": ca_cfg["name"]}),
                "ocel:qualifier": qualifier}))

        # W1: trace-hierarchy objects (author's design: TX + non-leaf CALLFRAMEs,
        # depth-based ids, one uniform contained-by rule)
        tx = df["txHash"].astype(str)
        depth = df["tracePosDepth"].astype(str)
        parent = _parent_depth(depth)
        tx_oid = "TX_" + tx
        # case membership: one TRANSACTION relation per event, direct, so no
        # event filter can sever an event from its transaction
        e2o_frames.append(pd.DataFrame({
            "ocel:eid": eid, "ocel:oid": tx_oid,
            "ocel:type": "TRANSACTION", "ocel:qualifier": "TRANSACTION"}))
        # sub-process structure: contained-by only inside non-root frames,
        # always targeting a CALLFRAME; a missing contained-by MEANS top-level
        inner = ~parent.isin(["", "1"])
        if inner.any():
            e2o_frames.append(pd.DataFrame({
                "ocel:eid": eid[inner],
                "ocel:oid": "FRAME_" + tx[inner] + "_" + parent[inner],
                "ocel:type": "CALLFRAME", "ocel:qualifier": "contained-by"}))
        objects_frames.append(pd.DataFrame({
            "ocel:oid": sorted(set(tx_oid)), "ocel:type": "TRANSACTION",
            "category": None, "account_type": None}))
        frames = pd.DataFrame({"tx": tx, "d": parent})
        frames = frames[~frames["d"].isin(["", "1"])].drop_duplicates()
        # a frame object exists for every parent of a logged event, and only for
        # those: an ancestor whose own operation is not in the log (another
        # category, a non-DApp call) has no direct event and is no object
        if not frames.empty:
            objects_frames.append(pd.DataFrame({
                "ocel:oid": "FRAME_" + frames["tx"] + "_" + frames["d"],
                "ocel:type": "CALLFRAME", "category": None, "account_type": None}))
            # frame nesting (2c): frame->frame contains only — TRANSACTION
            # objects appear in no O2O (membership is E2O, structure from ids).
            # A frame is contained by the nearest ancestor frame that is itself
            # an object; ancestors outside the log are skipped, so a missing
            # contains means top-level, as a missing contained-by does for events
            frame_ids = set(frames["tx"] + "_" + frames["d"])
            ancestor = _parent_depth(frames["d"])
            while True:
                unresolved = ~ancestor.isin(["", "1"]) & \
                    ~(frames["tx"] + "_" + ancestor).isin(frame_ids)
                if not unresolved.any():
                    break
                ancestor[unresolved] = _parent_depth(ancestor[unresolved])
            nested = ~ancestor.isin(["", "1"])
            if nested.any():
                o2o_frames.append(pd.DataFrame({
                    "ocel:oid": "FRAME_" + frames.loc[nested, "tx"] + "_"
                                + ancestor[nested],
                    "ocel:qualifier": "contains",
                    "ocel:oid_2": "FRAME_" + frames.loc[nested, "tx"] + "_"
                                  + frames.loc[nested, "d"]}))

    # ---- assemble, dedupe ----
    objects = pd.concat(objects_frames, ignore_index=True) if objects_frames \
        else pd.DataFrame(columns=["ocel:oid", "ocel:type", "category", "account_type"])
    objects = objects.drop_duplicates("ocel:oid").sort_values("ocel:oid").reset_index(drop=True)

    e2o = pd.concat(e2o_frames, ignore_index=True)

    # W2: same-entity mirroring, full equality, type-tag qualifiers. Every
    # mirror pair is mirrored from the same pre-mirror base, which is sound only
    # while no address is shared by two pairs; checked here (otherwise equality
    # would need a fixpoint pass).
    if variant in WORKAROUND_VARIANTS and object_model.get("w2_mirror_types"):
        pair_shared = [(t1, t2, typed_addresses.get(t1, set())
                        & typed_addresses.get(t2, set()))
                       for t1, t2 in _w2_pairs(object_model)]
        for i in range(len(pair_shared)):
            for j in range(i + 1, len(pair_shared)):
                overlap = pair_shared[i][2] & pair_shared[j][2]
                if overlap:
                    raise ValueError(
                        f"w2 mirror pairs {pair_shared[i][:2]} and "
                        f"{pair_shared[j][:2]} overlap on {len(overlap)} "
                        f"addresses (e.g. {sorted(overlap)[:3]}); single-pass "
                        "mirroring would not reach event-set equality")
        mirror_frames = []
        for t1, t2, shared in pair_shared:
            if not shared:
                continue
            c1, c2 = _type_cfg(object_model, t1), _type_cfg(object_model, t2)
            for src_cfg, dst_cfg in ((c1, c2), (c2, c1)):
                pref = src_cfg["id_prefix"]
                hit = e2o["ocel:oid"].str.startswith(pref) & \
                    e2o["ocel:oid"].str[len(pref):].isin(shared)
                if hit.any():
                    sub = e2o.loc[hit]
                    mirror_frames.append(pd.DataFrame({
                        "ocel:eid": sub["ocel:eid"],
                        "ocel:oid": dst_cfg["id_prefix"] + sub["ocel:oid"].str[len(pref):],
                        "ocel:type": dst_cfg["name"],
                        "ocel:qualifier": dst_cfg["name"]}))
        if mirror_frames:
            e2o = pd.concat([e2o] + mirror_frames, ignore_index=True)

    e2o = e2o.drop_duplicates(["ocel:eid", "ocel:oid", "ocel:qualifier"])
    act_ts = events.set_index("ocel:eid")[["ocel:activity", "ocel:timestamp",
                                           "timeStamp_ordered"]]
    e2o = e2o.join(act_ts, on="ocel:eid")
    e2o = e2o.sort_values(["timeStamp_ordered", "ocel:oid"]).drop(
        columns=["timeStamp_ordered"]).reset_index(drop=True)

    o2o = pd.concat(o2o_frames, ignore_index=True).drop_duplicates() if o2o_frames \
        else pd.DataFrame(columns=["ocel:oid", "ocel:qualifier", "ocel:oid_2"])
    o2o = o2o.reset_index(drop=True)

    object_changes = pd.concat(changes_frames, ignore_index=True) if changes_frames \
        else pd.DataFrame(columns=["ocel:oid", "ocel:type", "ocel:timestamp",
                                   "ocel:field", "state"])
    object_changes = object_changes.reset_index(drop=True)

    logger.info(f"compose_ocel[{variant}]: {len(events):,} events, "
                f"{len(objects):,} objects, {len(e2o):,} e2o, {len(o2o):,} o2o, "
                f"{len(object_changes):,} attribute changes")
    return {"events": events, "objects": objects, "e2o": e2o, "o2o": o2o,
            "object_changes": object_changes}
