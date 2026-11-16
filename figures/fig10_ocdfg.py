"""Figure 10 of the corrected paper: the object-centric directly-follows graph
of the dispute resolution process over markets, reporting EOAs and dispute
crowdsourcer tokens (the tokens that represent stakes in a dispute), as
frequency and performance graph.

The log is prepared the way regenerate_figures.py (Alessandro Berti, this
folder) does it: only events of operations that were not reverted; an event/object
pair counts once, even with several qualifiers; EOA objects only for accounts
whose recorded chain type is EOA; events in chain order (block, transaction
index, position in the call tree).

Selection, compared with the published figure:
  * object types MARKET, EOA and TOKEN. The published figure shows MARKET and
    ORDER; in the corrected log no event of the selected activities is related
    to an order, and the co-occurrence in the published log resulted from
    colliding event IDs;
  * nine activities: six of the published selection; 'delegate call to report'
    is left out (it carries no market), 'call to create dispute' and 'redeem
    dispute' do not occur in the log, and the event 'redeem dispute crowdsourcer'
    is replaced by the crowdsourcer's redemption call, which also carries the
    token; added are the token activities mint, burn and redemption;
  * arcs with at least 150 event couples are drawn, with the median duration in
    the performance graph.

Layout: the MARKET and TOKEN lanes determine the top-to-bottom layering; the
EOA arcs are drawn with constraint=false so that they do not pull activities
out of the process order. Activity names are wrapped and arc labels reduced to
the number, the colour giving the object type.

Writes ocdfg_market_eoa_token_v2_150_compact_{freq,perf}.pdf, cropped to a 3 pt
margin with pdfcrop (part of TeX Live), and the tables
ocdfg_market_eoa_token_v2_150_{nodes,edges}.csv (all arcs, with a 'shown' flag).
Requires Graphviz (dot) and pdfcrop on the PATH.

usage: fig10_ocdfg.py <log folder with events/objects/e2o.parquet> <out folder>
"""
import re
import subprocess
import sys
import textwrap
from pathlib import Path
from statistics import median

import duckdb
import pandas as pd
import pm4py
from pm4py.objects.ocel.obj import OCEL
from pm4py.visualization.ocel.ocdfg.variants import classic

log, out = Path(sys.argv[1]), Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)
NAME = 'ocdfg_market_eoa_token_v2_150'

ACTIVITIES = ['call to log initial report submission', 'create dispute', 'contribute to dispute',
              'complete dispute', 'call to log market finalization', 'redeem as initial reporter',
              'call to log dispute crowdsourcer tokens minted',
              'call to log dispute crowdsourcer tokens burned',
              'call to log dispute crowdsourcer redemption']
TYPES = ['MARKET', 'EOA', 'TOKEN']
LAYOUT_TYPES = {'MARKET', 'TOKEN'}
THRESHOLD = 150
COLOURS = {'MARKET': '#CC7900', 'EOA': '#009EC9', 'TOKEN': '#30DD40'}
classic.ot_to_color = lambda typ: COLOURS[typ]   # pm4py's own colours depend on the hash seed

c = duckdb.connect()
for t in ('events', 'objects', 'e2o'):
    c.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{log}/{t}.parquet')")
c.execute('''CREATE TEMP TABLE ev AS
    SELECT "ocel:eid" eid, "ocel:activity" activity, "ocel:timestamp" ts,
           "blockNumber" block_no, "transactionIndex" tx_index, "tracePos" trace_pos
    FROM events WHERE flag_reverted = false''')
c.execute('''CREATE TEMP TABLE rel AS
    SELECT DISTINCT r."ocel:eid" eid, r."ocel:oid" oid, r."ocel:type" typ
    FROM e2o r JOIN ev ON r."ocel:eid"=ev.eid
    JOIN objects o ON r."ocel:oid"=o."ocel:oid"
    WHERE r."ocel:type" IN ('EOA','MARKET','TOKEN')
      AND (r."ocel:type"<>'EOA' OR o.account_type='EOA')''')


def unconstrain(viz):
    """Edges of the types outside LAYOUT_TYPES get constraint=false, except the
    edges from a start marker (ellipse) or to an end marker (underline), which
    keep the markers above and below the activities."""
    keep = [COLOURS[t] for t in LAYOUT_TYPES]
    markers = set()
    for line in viz.body:
        m = re.match(r'\s*"([^"]+)" \[label=\S+ .*shape=(ellipse|underline)', line)
        if m:
            markers.add(m.group(1))
    body = []
    for line in viz.body:
        if '->' not in line:
            body.append(line)
            continue
        statements = []
        for stmt in line.split('\t'):          # several edges share one line
            ends = re.findall(r'"([^"]+)"', stmt)[:2]
            if '->' in stmt and not any(k in stmt for k in keep) and not (set(ends) & markers):
                stmt = re.sub(r'\]\s*$', ' constraint=false]', stmt)
            statements.append(stmt)
        body.append('\t'.join(statements))
    viz.body = body
    return viz


def compact(viz, wrap=16):
    """Column-shaped rendering: activity names wrapped, arc labels reduced to the
    number, tighter spacing, and an invisible edge from the first market activity
    to the first token activity so that the token chain lies beside the market chain."""
    body = []
    node_of = {}
    for line in viz.body:
        m = re.match(r'\s*"([^"]+)" \[label="([^"\n]+)\n(E=\d+)"', line)
        if m:                                  # frequency graph: name and E=<events>
            node_of[m.group(2)] = m.group(1)
            wrapped = '\\n'.join(textwrap.wrap(m.group(2), wrap))
            line = line.replace(f'label="{m.group(2)}\n{m.group(3)}"',
                                f'label="{wrapped}\\n{m.group(3)}"', 1)
        else:                                  # performance graph: name only
            m = re.match(r'\s*"([^"]+)" \[label="([^"\n]+)"', line)
            if m and m.group(2) not in COLOURS:
                node_of[m.group(2)] = m.group(1)
                wrapped = '\\n'.join(textwrap.wrap(m.group(2), wrap))
                line = line.replace(f'label="{m.group(2)}"', f'label="{wrapped}"', 1)
        if '->' in line:
            def short(mm):
                value = mm.group(2)
                return f'label="{int(value):,}"' if value.isdigit() else f'label="{value}"'
            line = re.sub(r'label="(\w+) EC=([^"]+)"', short, line)
        body.append(line)
    first_market = node_of.get('call to log initial report submission')
    first_token = node_of.get('call to log dispute crowdsourcer tokens minted')
    if first_market and first_token:
        body.append(f'\t"{first_market}" -> "{first_token}" [style=invis weight=2]')
    viz.body = ['\tgraph [nodesep=0.08 ranksep=0.25]', '\tnode [fontsize=13 margin="0.06,0.03"]'] + body
    return viz


relations = c.sql('''SELECT r.eid "ocel:eid", r.oid "ocel:oid", r.typ "ocel:type",
    ev.activity "ocel:activity", ev.ts "ocel:timestamp"
    FROM rel r JOIN ev USING(eid) WHERE r.typ IN (SELECT unnest(?))
    AND ev.activity IN (SELECT unnest(?)) ORDER BY block_no,tx_index,trace_pos,oid''',
    params=[TYPES, ACTIVITIES]).df()
events = relations[['ocel:eid', 'ocel:activity', 'ocel:timestamp']].drop_duplicates('ocel:eid')
objects = relations[['ocel:oid', 'ocel:type']].drop_duplicates()
model = pm4py.discover_ocdfg(OCEL(events=events, objects=objects, relations=relations))
print(f"{len(events)} events, objects {objects['ocel:type'].value_counts().to_dict()}")

rows = []
for typ, pairs in model['edges']['event_couples'].items():
    for (src, dst), couples in pairs.items():
        perf = model['edges_performance']['event_couples'][typ][(src, dst)]
        rows.append((typ, src, dst, len(couples), median(perf), len(couples) >= THRESHOLD))
pd.DataFrame(rows, columns=['type', 'source', 'target', 'event_couples', 'median_seconds', 'shown']
             ).sort_values(['type', 'source', 'target']).to_csv(out / f'{NAME}_edges.csv', index=False)
events['ocel:activity'].value_counts().rename_axis('activity').reset_index(name='events'
    ).sort_values('activity').to_csv(out / f'{NAME}_nodes.csv', index=False)

for annotation, suffix in (('frequency', 'freq'), ('performance', 'perf')):
    viz = classic.apply(model, parameters={
        'format': 'pdf', 'rankdir': 'TB', 'annotation': annotation,
        classic.Parameters.PERFORMANCE_AGGREGATION_MEASURE: 'median',
        'act_metric': 'events', 'edge_metric': 'event_couples', 'edge_threshold': THRESHOLD})
    viz = compact(unconstrain(viz))
    pdf = out / f'{NAME}_compact_{suffix}.pdf'
    pdf.write_bytes(viz.pipe(format='pdf'))
    subprocess.run(['pdfcrop', '--margins', '3', pdf, pdf], check=True, capture_output=True)
print("wrote", out / f'{NAME}_compact_freq.pdf', "and", out / f'{NAME}_compact_perf.pdf')
