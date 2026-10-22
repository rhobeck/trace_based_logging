"""Parquet part-file datasets with checkpointing.

Each processing stage writes its output as numbered part files inside a dataset
directory:

    <stage_dir>/<dataset>/part-00001.parquet
                         /part-00002.parquet
                         ...

A part file is an atomic checkpoint: it is written to a temporary name and renamed into
place only once complete, so a crashed run never leaves a half-written part, and a rerun
resumes at the first missing part. A whole dataset reads back as one DataFrame (or
lazily via DuckDB over the directory glob).

Design: identity is carried by real columns, never by a DataFrame index; no silent
gaps (parts are either complete or absent); parquet instead of pickle, which is
neither versioned nor portable. CSV is available only as an explicit export.

The discovery checkpoint is split by size: small frontier state (level, contract sets)
as JSON; the transaction-hash set (hundreds of thousands of entries) as a parquet
sidecar. The JSON is written last and atomically, so its presence marks a valid
checkpoint pair.
"""

import json
import os
import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CHECKPOINT_JSON = "_checkpoint.json"
CHECKPOINT_TXS = "_checkpoint_transactions.parquet"
# parquet schema-metadata key under which discovery files record the block range
# they were produced for (reused files are checked against the run's range)
BLOCK_RANGE_KEY = b"block_range"


def dataset_dir(base_dir, dataset_name):
    d = os.path.join(base_dir, dataset_name)
    os.makedirs(d, exist_ok=True)
    return d


def part_path(base_dir, dataset_name, index):
    return os.path.join(dataset_dir(base_dir, dataset_name), f"part-{index:05d}.parquet")


def completed_parts(base_dir, dataset_name):
    """Sorted list of part indices already fully written for a dataset."""
    d = os.path.join(base_dir, dataset_name)
    if not os.path.isdir(d):
        return []
    idx = []
    for name in os.listdir(d):
        if name.startswith("part-") and name.endswith(".parquet"):
            idx.append(int(name[len("part-"):-len(".parquet")]))
    return sorted(idx)


def write_part(base_dir, dataset_name, df, index):
    """Atomically write one part file (temp name, then rename into place)."""
    final = part_path(base_dir, dataset_name, index)
    tmp = final + ".tmp"
    df.to_parquet(tmp, index=False)
    os.replace(tmp, final)
    return final


def read_part(base_dir, dataset_name, index, columns=None):
    return pd.read_parquet(part_path(base_dir, dataset_name, index), columns=columns)


def read_dataset(base_dir, dataset_name, columns=None):
    """Read all parts of a dataset into a single DataFrame (empty if none)."""
    parts = completed_parts(base_dir, dataset_name)
    if not parts:
        return pd.DataFrame()
    frames = [read_part(base_dir, dataset_name, i, columns=columns) for i in parts]
    return pd.concat(frames, ignore_index=True)


def reset_dataset(base_dir, dataset_name):
    """Delete a dataset directory (use when a stage must start clean)."""
    d = os.path.join(base_dir, dataset_name)
    if os.path.isdir(d):
        shutil.rmtree(d)


def write_atomic_parquet(path, df, block_range=None):
    """Atomic single-file parquet write (temp name, then rename). With block_range
    (min_block, max_block), the range is recorded in the file's schema metadata."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp = path + ".tmp"
    if block_range is None:
        df.to_parquet(tmp, index=False)
    else:
        table = pa.Table.from_pandas(df, preserve_index=False)
        metadata = dict(table.schema.metadata or {})
        metadata[BLOCK_RANGE_KEY] = json.dumps([int(b) for b in block_range]).encode()
        pq.write_table(table.replace_schema_metadata(metadata), tmp)
    os.replace(tmp, path)
    return path


def parquet_block_range(path):
    """The (min_block, max_block) a parquet file was written for, or None if it
    records none (files from before block ranges were recorded)."""
    raw = (pq.read_schema(path).metadata or {}).get(BLOCK_RANGE_KEY)
    return tuple(json.loads(raw)) if raw else None


# --------------------------- discovery checkpoint ---------------------------

def save_checkpoint(base_dir, state, block_range=None):
    """Persist the discovery frontier so an interrupted run can resume.

    Small frontier state goes to JSON; the (large) set of seen transaction hashes goes
    to a parquet sidecar. The JSON is written last: its presence marks validity.
    block_range (min_block, max_block), if given, is recorded in the JSON.
    """
    os.makedirs(base_dir, exist_ok=True)
    write_atomic_parquet(
        os.path.join(base_dir, CHECKPOINT_TXS),
        pd.DataFrame({"txHash": sorted(state["all_transactions"])}),
    )
    payload = {
        "level": state["level"],
        "base_contract": state["base_contract"],
        "contracts_dapp": sorted(state["contracts_dapp"]),
        "contracts_lx": sorted(state["contracts_lx"]),
        "contracts_non_dapp": sorted(state["contracts_non_dapp"]),
    }
    if block_range is not None:
        payload["block_range"] = [int(b) for b in block_range]
    path = os.path.join(base_dir, CHECKPOINT_JSON)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f)
    os.replace(tmp, path)
    return path


def load_checkpoint(base_dir):
    """Load a saved discovery frontier, or None if there is none. A recorded block
    range comes back under "block_range" (absent in checkpoints that predate it)."""
    path = os.path.join(base_dir, CHECKPOINT_JSON)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        payload = json.load(f)
    for key in ("contracts_dapp", "contracts_lx", "contracts_non_dapp"):
        payload[key] = set(payload[key])
    txs_path = os.path.join(base_dir, CHECKPOINT_TXS)
    if os.path.exists(txs_path):
        payload["all_transactions"] = set(pd.read_parquet(txs_path)["txHash"])
    else:
        payload["all_transactions"] = set()
    return payload
