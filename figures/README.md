# Figures of the corrected paper

The data and the graphs behind Figures 5–10 of the corrected paper, computed on the
corrected event log (the relational tables in `transformation/log_paper` of a run).
Figures 5–9 are drawn in the paper's LaTeX source from the CSV files; Figure 10 is
included as PDF.

Figures 5–9 repeat the analyses of the published paper on the corrected event log.
Figure 10 shows the dispute resolution process over markets, EOAs, and dispute
crowdsourcer tokens (the tokens that represent stakes in a dispute). The published
Figure 10 shows markets and orders. In the corrected event log, no event of the selected
activities is related to an order (`dispute_objects` in
`output/regenerate_figures/summary.json`); the co-occurrence in the published event log
resulted from colliding event IDs.

| Step | Script | Output |
|---|---|---|
| 1 | `regenerate_figures.py` (Alessandro Berti) | `output/regenerate_figures/`: the histograms of Figures 5–9 as full distributions (`full/`), analysis totals (`summary.json`), and a per-bin comparison with the published figures (`histogram_deltas.csv`). In the layout of the published figures, and not used by the corrected paper: the ten most frequent bins (`data/`) and the graphs over the published selection of Figure 10 (`figures/`, `full/dispute_*.csv`) |
| 2 | `fig5-9_data.py` | `output/paper/*.csv`: the files from which the corrected paper draws Figures 5–9 |
| 3 | `fig10_ocdfg.py` | `output/paper/ocdfg_*`: Figure 10 as PDF, with its node and arc tables |

`fig5-9_data.py` reads only the output of step 1. It uses the full distributions and adds
the bars the figures show beyond them: a bin 0 in Figure 5 (objects whose events lie
in one block), a bar at 0 in Figure 7 (EOAs without a market), and one tail bar for the
values above 100 in Figures 7 and 8. Both zero counts are taken from `summary.json`.

## Running

From the repository root, with the project dependencies plus `duckdb==2.0.0`, and with
Graphviz (`dot`) and TeX Live's `pdfcrop` on the PATH:

```console
LOG=/path/to/transformation/log_paper
PUBLISHED=/path/to/published/paper
DEPS="--with duckdb==2.0.0 --with pandas==3.1.0 --with pm4py==2.7.23.11"

uv run --no-project $DEPS python figures/regenerate_figures.py --log $LOG \
    --output figures/output/regenerate_figures --baseline $PUBLISHED
python3 figures/fig5-9_data.py figures/output/regenerate_figures figures/output/paper
uv run --no-project $DEPS python figures/fig10_ocdfg.py $LOG figures/output/paper
```

`--baseline` is optional: it points at the sources of the published paper (its `data/`
folder) and only feeds `histogram_deltas.csv`.

The files in `output/paper/` are the ones used in the corrected paper.

## Step 1 in detail: `regenerate_figures.py`

The analysis reads the relational parquet tables. All outputs stay in the output
folder: the histograms, the full distributions, graph counts and median durations,
and analysis totals. Optional `--baseline /path/to/original/paper` produces per-bin
comparisons.

The script excludes reverted events before deriving co-occurrences, deduplicates
event/object pairs, uses the recorded account type for EOA analyses, and excludes
market addresses from contract counterparts. Durations use block timestamps;
within-block order uses transaction index and trace position. Histograms retain
at most ten most frequent bins and the duration panel uses positive lifetimes
(month = 30 days, year = 365 days). These display conventions reconstruct the
published panels because their original analysis code is unavailable. Graphs
use the paper's selected activities and MARKET/ORDER types, median performance,
and a 50-event-couple display threshold; missing types are not manufactured.
