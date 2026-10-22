"""Market information propagated from a market's creation to its later events
(transformation_augur_utils.propagate_extraInfo / propagate_marketType; used on
the sensitive_events branch of the Augur transformation). Both return one lookup
row per created market, keyed by ``market``, which the transformation merges onto
the events."""
import math
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.trace_based_logging.log_construction import transformation_augur_utils as utils


def events():
    """Two market creations with extraInfo and marketType, three later events on
    the markets, one event without a market."""
    return pd.DataFrame({
        "Activity": ["create market", "update market", "update market", "create market",
                     "update market", "non-market event"],
        "market": ["A", "B", "A", "B", "B", None],
        "extraInfo": ['{"longDescription":"entry1A","resolutionSource":"entry2A","tags":"entry3A"}',
                      None, None,
                      '{"longDescription":"entry1B","resolutionSource":"entry2B","tags":"entry3B"}',
                      None, None],
        "marketType": [1, None, None, 2, None, None]})


@pytest.mark.xfail(strict=True, reason=(
    "propagate_extraInfo attaches the market ids by position after reset_index, but "
    "pandas >= 3 json_normalize keeps the Series index, so the parsed extraInfo lands "
    "on the wrong market or on none (TODO: 'propagate_extraInfo misaligns market "
    "information'). Latent: only the sensitive_events branch calls it."))
def test_propagate_extraInfo_gives_one_row_per_created_market():
    info = utils.propagate_extraInfo(events())
    assert set(info["market"]) == {"A", "B"} and len(info) == 2
    merged = pd.merge(events(), info, on="market", how="left")
    assert merged["resolutionSource"][0] == "entry2A"
    assert merged["longDescription"][2] == "entry1A"        # propagated to a later event of A
    assert merged["longDescription"][3] == "entry1B"
    assert isinstance(merged["resolutionSource"][5], float) and math.isnan(merged["resolutionSource"][5])


@pytest.mark.xfail(strict=True, reason=(
    "propagate_marketType renames the numeric market types through chained assignment "
    "of strings into a float column, which pandas >= 3 rejects (LossySetitemError; same "
    "TODO entry). Latent: only the sensitive_events branch calls it."))
def test_propagate_marketType_names_the_type_per_market():
    types = utils.propagate_marketType(events())
    assert dict(zip(types["market"], types["marketType"])) == {"A": "categorical", "B": "scalar"}
    merged = pd.merge(events(), types, on="market", how="left", suffixes=("", "_propagated"))
    assert merged["marketType_propagated"][2] == "categorical"      # later event of A
    assert merged["marketType_propagated"][4] == "scalar"
    assert isinstance(merged["marketType_propagated"][5], float) \
        and math.isnan(merged["marketType_propagated"][5])
