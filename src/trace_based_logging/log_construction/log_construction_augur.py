"""Augur orchestrator for the OCEL 2.0 log construction: loads the Augur mapping m
from mappings.json, runs the generic engine (ocel_construction, Algorithm 1 of the
paper) and exports the result with pm4py's OCEL 2.0 writers. The published log is
the variant 'paper'; the variants A and B exist for the tests (see
ocel_construction)."""
import functools
import json
import os
import pandas as pd
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from src.trace_based_logging.logging_config import setup_logging
from src.trace_based_logging import storage
from src.trace_based_logging.decoding import sanitize_for_parquet
from src.trace_based_logging.config import build_node_url
from src.trace_based_logging.log_construction import ocel_construction
from src.trace_based_logging.log_construction import address_classification

logger = setup_logging()

OCEL_TABLES = ("events", "objects", "e2o", "o2o", "object_changes")


def load_object_model(mappings_path=None):
    if mappings_path is None:
        mappings_path = os.path.join(os.path.dirname(__file__), "mappings.json")
    with open(mappings_path) as fh:
        object_model = json.load(fh)["object_model"]
    ocel_construction.validate_object_model(object_model)
    return object_model


def load_overall_dataframe(resources_dir, log_folder, selected_datasets, desired_columns, CONFIG):
    """Concatenate the selected transformed category tables into the engine's
    operation table (parquet; pickle is read when present).

    desired_columns: None loads all columns, which the log needs since every
    column becomes an event attribute; a column list restricts the load for
    narrow analyses. The concatenation reindexes: event identity never comes
    from a row index.
    """
    overall_dataframe_list = []

    for ds in selected_datasets:
        base = os.path.join(resources_dir, log_folder, "transformation",
                            f'{ds}_{CONFIG["base_contract"]}_{CONFIG["min_block"]}_{CONFIG["max_block"]}')
        if os.path.exists(base + ".parquet"):
            df = pd.read_parquet(base + ".parquet")
        elif os.path.exists(base + ".pkl"):
            df = pd.read_pickle(base + ".pkl")
        else:
            raise FileNotFoundError(f"Transformed dataset missing: {base}.parquet")
        if desired_columns is not None:
            df = df[[c for c in df.columns if c in desired_columns]]
        overall_dataframe_list.append(df)
        logger.info(f"Loaded {ds}: {len(df):,} rows")

    overall_dataframe = pd.concat(overall_dataframe_list, ignore_index=True)
    logger.info(f"Combined overall dataframe shape: {overall_dataframe.shape}")
    return overall_dataframe


def create_ocel_files(overall_dataframe, RESOURCES_DIR, CONFIG, variant="A",
                      object_model=None, account_types=None):
    """Creates and stores the OCEL objects, events, relations, o2o, and attribute
    histories for one variant, and builds an OCEL object using pm4py.

    The construction itself is ocel_construction.compose_ocel, Algorithm 1 with
    the mapping m; this function adds the checkpointing of the relational tables
    and builds the pm4py OCEL object.
    """
    if object_model is None:
        object_model = load_object_model()
    if account_types is None:
        raise ValueError("account_types table is required (see build_log)")

    # Checkpointed in two stages: the composed relational tables are persisted
    # as parquet the moment they exist (and freed); the pm4py OCEL assembly then
    # reads them back, so a rerun after a crash (or a failed export) skips the
    # expensive composition entirely.
    out_dir = os.path.join(RESOURCES_DIR, CONFIG["log_folder"], "transformation",
                           f"log_{variant}")
    if all(os.path.exists(os.path.join(out_dir, f"{n}.parquet")) for n in OCEL_TABLES):
        logger.info(f"Log {variant}: relational tables found at {out_dir} — "
                    "skipping composition (checkpoint).")
        tables = {n: pd.read_parquet(os.path.join(out_dir, f"{n}.parquet"))
                  for n in OCEL_TABLES}
    else:
        tables = ocel_construction.compose_ocel(
            overall_dataframe, object_model, variant=variant,
            account_types=account_types)
        for name in OCEL_TABLES:
            tables[name] = sanitize_for_parquet(tables[name])
            storage.write_atomic_parquet(os.path.join(out_dir, f"{name}.parquet"),
                                         tables[name])
        logger.info(f"Log {variant}: relational tables written to {out_dir}")

    from pm4py.objects.ocel.obj import OCEL
    ocel = OCEL(events=tables["events"], objects=tables["objects"],
                relations=tables["e2o"], o2o=tables["o2o"],
                object_changes=tables["object_changes"])
    logger.info("OCEL object created successfully.")
    return ocel


def save_ocel_xml(ocel, resources_dir, log_folder, base_contract, min_block, max_block,
                  variant="A", export_xml=True):
    """Saves the OCEL object via pm4py's OCEL **2.0** writers: SQLite always (the
    relational-primary pm4py form), XML-OCEL optionally (derived view). pm4py's
    OCEL 2.0 writers keep the qualifiers of the relations; the OCEL 1.0 writer
    does not.
    """
    import pm4py
    # the published log, variant 'paper', carries no variant letter in its file names
    tag = "" if variant == "paper" else f"{variant}_"
    file_base = f"ocel_{tag}{base_contract}_{min_block}_{max_block}"
    target_base = os.path.join(resources_dir, log_folder, "transformation", file_base)
    if not os.path.exists(target_base + ".sqlite"):
        pm4py.write_ocel2(ocel, target_base + ".sqlite")
        logger.info(f"OCEL 2.0 SQLite written: {target_base}.sqlite")
    if export_xml:
        try:
            pm4py.write_ocel2(ocel, target_base + ".xmlocel")
            logger.info(f"OCEL 2.0 XML written: {target_base}.xmlocel")
        except Exception as e:
            # build-don't-die policy: the XML is a derived view; SQLite and the
            # parquet tables are the artifacts of record
            logger.error(f"XML export failed ({e}); SQLite and parquet tables stand.")


def classify_run_accounts(overall_dataframe, object_model, RESOURCES_DIR, LOG_FOLDER,
                          CONFIG, get_code=None):
    """EOA/CA typing of the run's own object addresses (Table 3), checkpointed as
    transformation/account_types_<suffix>.parquet; the trace evidence comes from
    the run's extraction trace tree, the code sizes from the node at max_block.
    A checkpointed table is reused only if it covers every object address (a
    changed mapping m can add some); otherwise the run classifies again, with the
    getCode cache sparing the addresses already queried."""
    suffix = f"{CONFIG['base_contract']}_{CONFIG['min_block']}_{CONFIG['max_block']}"
    out_dir = os.path.join(RESOURCES_DIR, LOG_FOLDER, "transformation")
    out_path = os.path.join(out_dir, f"account_types_{suffix}.parquet")
    if os.path.exists(out_path):
        account_types = pd.read_parquet(out_path)
        missing = address_classification.collect_addresses(
            overall_dataframe, object_model).difference(account_types["address"])
        if not missing:
            logger.info(f"account types found at {out_path} — skipping classification")
            return account_types
        logger.info(f"account types at {out_path} miss {len(missing):,} object "
                    "addresses — classifying again")
    trace_path = os.path.join(RESOURCES_DIR, LOG_FOLDER, "extraction",
                              f"df_trace_tree_{suffix}.parquet")
    trace_tree = pd.read_parquet(trace_path, columns=["type", "tracePosDepth",
                                                      "from", "to", "address"])
    if get_code is None:
        get_code = functools.partial(address_classification.get_code_size,
                                     node_url=build_node_url(CONFIG))
    account_types = address_classification.classify_accounts(
        overall_dataframe, object_model, trace_tree, CONFIG["max_block"], get_code,
        cache_path=os.path.join(out_dir, f"getcode_cache_{suffix}.parquet"),
        known_contracts=CONFIG.get("contracts_dapp", ()))
    storage.write_atomic_parquet(out_path, account_types)
    logger.info(f"account types written to {out_path}")
    return account_types


def build_log(RESOURCES_DIR, LOG_FOLDER, CONFIG, account_types_path=None,
              variants=("paper",), export_xml=True,
              excluded_columns=("input", "output", "data", "topics"),
              get_code=None):
    """excluded_columns: raw payload blobs that are not event attributes (their
    decoded forms are) — loading them for 11M rows exceeds commodity memory.

    account_types_path: a recorded account-type table to use as-is. Without it,
    the run classifies its own
    addresses (address_classification) against the node; get_code overrides the
    node query (tests)."""
    
    # Mapping toggle keys to corresponding dataset base filenames
    toggle_to_filename = {
        "dapp_events": "dapp_events",
        "dapp_calls": "dapp_calls",
        "dapp_delegatecalls": "dapp_delegatecalls",
        "dapp_zero_value_calls": "dapp_zero_value_calls",
        "dapp_creations": "dapp_creations",
        "non_dapp_events": "non_dapp_events",
        "non_dapp_calls": "non_dapp_calls",
        "non_dapp_delegatecalls": "non_dapp_delegatecalls",
        "non_dapp_zero_value_calls": "non_dapp_zero_value_calls",
        "non_dapp_creations": "non_dapp_creations"
    }
    
    selected_datasets = [toggle_to_filename[key] for key, value in CONFIG.items()
                         if value and key in toggle_to_filename]
    logger.info(f"Datasets selected for construction: {selected_datasets}")
    
    # Load and concatenate the overall dataframe: every column EXCEPT the raw
    # payload blobs (excluded_columns) — attribute completeness without loading
    # multi-KB hex strings per operation.
    desired = None
    if excluded_columns:
        import pyarrow.parquet as _pq
        desired = set()
        for ds in selected_datasets:
            p = os.path.join(RESOURCES_DIR, LOG_FOLDER, "transformation",
                             f"{ds}_{CONFIG['base_contract']}_{CONFIG['min_block']}_{CONFIG['max_block']}.parquet")
            if os.path.exists(p):
                desired |= set(_pq.ParquetFile(p).schema_arrow.names)
        desired -= set(excluded_columns)
    overall_dataframe = load_overall_dataframe(RESOURCES_DIR, LOG_FOLDER,
                                               selected_datasets, desired, CONFIG)
    
    object_model = load_object_model()
    if account_types_path is not None:
        account_types = pd.read_parquet(account_types_path)
    else:
        account_types = classify_run_accounts(overall_dataframe, object_model,
                                              RESOURCES_DIR, LOG_FOLDER, CONFIG,
                                              get_code=get_code)

    # Build and save one log per requested variant.
    results = {}
    for variant in variants:
        ocel_obj = create_ocel_files(overall_dataframe, RESOURCES_DIR, CONFIG,
                                     variant=variant, object_model=object_model,
                                     account_types=account_types)
        save_ocel_xml(ocel_obj, RESOURCES_DIR, LOG_FOLDER, CONFIG["base_contract"],
                      CONFIG["min_block"], CONFIG["max_block"], variant=variant,
                      export_xml=export_xml)
        results[variant] = {name: len(getattr(ocel_obj, attr))
                            for name, attr in [("events", "events"),
                                               ("objects", "objects"),
                                               ("e2o", "relations"),
                                               ("o2o", "o2o"),
                                               ("object_changes", "object_changes")]}
    
    logger.info(f"Log building completed: {results}")
    return results
