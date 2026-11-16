"""Writes the CSV files from which the corrected paper draws Figures 5 to 9.

Every number comes from the output of regenerate_figures.py (Alessandro Berti,
this folder), run on the corrected log; this script reads no log itself.
It takes the full distributions (folder full/) instead of the ten most frequent
bins (folder data/) and adds what the full figures show beyond them:

  Figure 5  a bin 0 for the objects whose events all lie in one block
            (lifetime zero, which regenerate_figures.py leaves out of the
            histogram and reports as zero_duration in summary.json);
  Figure 7  a bar at 0 for the active EOAs without any market
            (without_counterpart of eoa_market in summary.json), and the values
            above 100 combined into one tail bar;
  Figure 8  the EOA series with the values above 100 combined into one tail bar.

Figures 6, 8 (contracts) and 9 use the full distributions unchanged.

The tail bar sits at x = 106; the third column of the *_cut.csv files gives the
range a bar stands for. pgfplots skips CSV rows that start with $, so the
duration labels use \\( \\) instead of $ $.

usage: fig5-9_data.py <regenerate_figures output folder> <out folder>
"""
import csv
import json
import shutil
import sys
from pathlib import Path

results, out = Path(sys.argv[1]), Path(sys.argv[2])
full = results / "full"
summary = json.loads((results / "summary.json").read_text())
out.mkdir(parents=True, exist_ok=True)

CUT, TAIL_X = 100, 106


def read(name):
    with open(full / f"{name}.csv", newline="") as f:
        return [row for row in csv.reader(f)][1:]


def write(name, header, rows):
    with open(out / f"{name}.csv", "w", newline="") as f:
        csv.writer(f, lineterminator="\n").writerows([header] + rows)


# Figure 6, Figure 8 (contracts per market), Figure 9: the full distributions as they are
for name in ("eoa_nunique_activities", "market_nunique_activities", "order_nunique_activities",
             "eoa_market", "market_eoa", "contract_market", "order_contract"):
    shutil.copyfile(full / f"{name}.csv", out / f"{name}.csv")

# Figure 5: lifetimes in six bins, preceded by the bin 0 of zero lifetimes
for typ in ("EOA", "ORDER"):
    name = f"{typ.lower()}_lifecycle_duration"
    rows = [[label.replace("$", "\\(", 1).replace("$", "\\)", 1), y] for label, y in read(name)]
    write(name, ["x", "y"], [[0, int(summary["lifecycle"][typ]["zero_duration"])]] + rows)


# Figures 7 and 8: values up to 100, one tail bar for the rest
def cut(name, zero=None):
    counts = {int(x): int(y) for x, y in read(name)}
    rows = [] if zero is None else [[0, zero, 0]]
    rows += [[x, y, x] for x, y in sorted(counts.items()) if x <= CUT]
    rows.append([TAIL_X, sum(y for x, y in counts.items() if x > CUT), f"{CUT + 1}–{max(counts)}"])
    write(f"{name}_cut", ["x", "y", "range"], rows)


cut("eoa_market", zero=summary["interactions"]["eoa_market"]["without_counterpart"])
cut("market_eoa")
print("wrote", sorted(p.name for p in out.glob("*.csv")))
