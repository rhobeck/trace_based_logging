"""The output folders of a run and the single-file form of the extraction output."""
import os

import pandas as pd

from src.trace_based_logging.logging_config import setup_logging
from src.trace_based_logging import storage

logger = setup_logging()


def folder_set_up(dir_path, config):
    for folder in ["extraction", "decoding", "transformation"]:
        os.makedirs(os.path.join(dir_path, "resources", config["log_folder"], folder),
                    exist_ok=True)


def save_trace_data(config, state, dir_path):
    """Persist the combined extraction output under the established naming scheme.

    Storage is parquet. The per-batch part files written during extraction remain
    the resumable record; these files are the single-file form for the later
    stages.
    """
    base_contract = state["base_contract"]
    out_dir = os.path.join(dir_path, "resources", config["log_folder"], "extraction")
    suffix = f"{base_contract}_{config['min_block']}_{config['max_block']}"

    trace_path = os.path.join(out_dir, f"df_trace_tree_{suffix}.parquet")
    storage.write_atomic_parquet(trace_path, state["trace_tree"])

    contracts_df = pd.DataFrame({"address": sorted(state["contracts_dapp"])})
    storage.write_atomic_parquet(
        os.path.join(out_dir, f"contracts_dapp_{suffix}.parquet"), contracts_df)
    # human-readable copy
    with open(os.path.join(out_dir, f"contracts_dapp_{suffix}.txt"), "w") as output:
        output.write("\n".join(sorted(state["contracts_dapp"])))

    logger.info(f"DONE WITH THE DATA EXTRACTION, TRACE FILES SAVED ({trace_path})")
