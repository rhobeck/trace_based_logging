"""Reproduce the corrigendum's nine histogram inputs and two OC-DFGs.

Requires the normal project dependencies plus duckdb==2.0.0 and Graphviz.
Example (from this repository):
  python regenerate_figures.py --log ../data/log/relational_tables \
      --paper ../65f02e3d1f5694e813bf69bb --output ../figure_results \
      --baseline ../work/before --write-paper

No source data are changed. Only existing CSV/PDF assets are replaced in the paper.
The old plotting code is unavailable: the ten most frequent bins and positive
lifetimes reconstruct its displayed scope. A month is 30 days, a year 365 days.
All full distributions and zero-duration counts are also exported for transparency.
"""
import argparse
import json
import shutil
from pathlib import Path
from statistics import median

import duckdb
import pandas as pd
import pm4py
from pm4py.objects.ocel.obj import OCEL
from pm4py.visualization.ocel.ocdfg.variants import classic

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--log', type=Path, required=True)
p.add_argument('--paper', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--baseline', type=Path, help='Optional original paper assets for comparisons')
p.add_argument('--write-paper', action='store_true')
a = p.parse_args()
for sub in ('data', 'figures', 'full'):
    (a.output / sub).mkdir(parents=True, exist_ok=True)
c = duckdb.connect()
c.execute("SET threads=16; SET memory_limit='24GB'")
for t in ('events', 'objects', 'e2o', 'o2o', 'object_changes'):
    c.read_parquet(str(a.log / (t + '.parquet'))).create_view(t)

# Filter events first. Relation qualifiers must not multiply an event/object pair.
c.execute('''CREATE TEMP TABLE ev AS
    SELECT "ocel:eid" eid, "ocel:activity" activity, "ocel:timestamp" ts,
           "blockNumber" block_no, "transactionIndex" tx_index, "tracePos" trace_pos
    FROM events WHERE flag_reverted = false''')
c.execute('''CREATE TEMP TABLE active_objects AS
    SELECT o.* FROM objects o SEMI JOIN (
        SELECT DISTINCT r."ocel:oid" oid FROM e2o r JOIN ev ON r."ocel:eid"=ev.eid
    ) r ON o."ocel:oid"=r.oid''')
# The supplied EOA type also contains contract accounts: use the recorded chain type.
c.execute('''CREATE TEMP TABLE rel AS
    SELECT DISTINCT r."ocel:eid" eid, r."ocel:oid" oid, r."ocel:type" typ
    FROM e2o r JOIN ev ON r."ocel:eid"=ev.eid
    JOIN active_objects o ON r."ocel:oid"=o."ocel:oid"
    WHERE r."ocel:type" IN ('EOA','ORDER','MARKET','CONTRACT')
      AND (r."ocel:type"<>'EOA' OR o.account_type='EOA')''')
c.execute('''CREATE TEMP VIEW effective_changes AS SELECT h.* FROM object_changes h
    SEMI JOIN active_objects o ON h."ocel:oid"=o."ocel:oid"''')
c.execute('''CREATE TEMP VIEW effective_o2o AS SELECT r.* FROM o2o r
    SEMI JOIN active_objects a ON r."ocel:oid"=a."ocel:oid"
    SEMI JOIN active_objects b ON r."ocel:oid_2"=b."ocel:oid"''')
# Both Figures 8 and 9 use non-market CONTRACT counterparts, by distinct address.
c.execute('''CREATE TEMP TABLE counterparts AS SELECT eid, oid FROM rel r
    WHERE typ='CONTRACT' AND NOT EXISTS (
        SELECT 1 FROM objects m WHERE m."ocel:type"='MARKET'
        AND substr(m."ocel:oid",8)=substr(r.oid,10))''')
c.execute('''CREATE TEMP TABLE lifecycles AS
    SELECT typ, oid, count(DISTINCT activity) n,
           epoch(max(ts)-min(ts)) duration
    FROM rel JOIN ev USING(eid) WHERE typ IN ('EOA','ORDER','MARKET')
    GROUP BY typ,oid''')

stats = {
    'events_total': c.sql('SELECT count(*) FROM events').fetchall()[0][0],
    'events_effective': c.sql('SELECT count(*) FROM ev').fetchall()[0][0],
    'inactive_objects': dict(c.sql('''SELECT o."ocel:type",count(*) FROM objects o
        ANTI JOIN active_objects a ON o."ocel:oid"=a."ocel:oid" GROUP BY 1''').fetchall()),
    'excluded_contract_accounts_labelled_EOA': c.sql('''SELECT count(*) FROM active_objects
        WHERE "ocel:type"='EOA' AND account_type='CA' ''').fetchall()[0][0],
    'effective_o2o': c.sql('SELECT count(*) FROM effective_o2o').fetchall()[0][0],
    'effective_object_changes': c.sql('SELECT count(*) FROM effective_changes').fetchall()[0][0],
    'lifecycle': {}, 'interactions': {},
}
deltas = []

def histogram(name, frame, top=True):
    """Keep the paper's ten most frequent bins; retain all bins separately."""
    frame = frame.sort_values('x').reset_index(drop=True) if top else frame
    frame.to_csv(a.output / 'full' / (name + '.csv'), index=False)
    shown = frame.sort_values(['y', 'x'], ascending=[False, True]).head(10).sort_values('x') if top else frame
    shown.to_csv(a.output / 'data' / (name + '.csv'), index=False)
    if a.baseline:
        old = pd.read_csv(a.baseline / 'data' / (name + '.csv'), dtype={'x': str})
        now = dict(zip(frame.x.astype(str), frame.y))
        shown_x = set(shown.x.astype(str))
        # An omitted published bin has an unknown count, not a count of zero.
        for x, y in old.itertuples(index=False, name=None):
            new = int(now.get(x, 0))
            deltas.append([name, x, int(y), new, new-int(y), x in shown_x])
    print(name, shown.to_dict('records'), flush=True)

for typ in ('EOA', 'MARKET', 'ORDER'):
    histogram(typ.lower()+'_nunique_activities', c.sql(
        'SELECT n x,count(*) y FROM lifecycles WHERE typ=? GROUP BY n', params=[typ]).df())
    stats['lifecycle'][typ] = c.sql('''SELECT count(*) objects,min(n) min_activities,
        max(n) max_activities,count(*) FILTER(WHERE duration=0) zero_duration,
        count(*) FILTER(WHERE duration>604800) over_week,
        count(*) FILTER(WHERE duration>2592000) over_month,
        max(duration) max_duration_seconds
        FROM lifecycles WHERE typ=?''', params=[typ]).df().iloc[0].to_dict()
labels = [r'$\le$1~H','1~H-1~D','1~D-1~W','1~W-1~M','1~M-1~Y',r'$>$1~Y']
for typ in ('EOA','ORDER'):
    counts = dict(c.sql('''SELECT CASE WHEN duration<=3600 THEN 0
        WHEN duration<=86400 THEN 1 WHEN duration<=604800 THEN 2
        WHEN duration<=2592000 THEN 3 WHEN duration<=31536000 THEN 4 ELSE 5 END bin,
        count(*) FROM lifecycles WHERE typ=? AND duration>0 GROUP BY bin''', params=[typ]).fetchall())
    # Preserve semantic bin order, not lexicographic order of the TeX labels.
    frame = pd.DataFrame({'x': labels, 'y': [counts.get(i,0) for i in range(6)]})
    histogram(typ.lower()+'_lifecycle_duration', frame, top=False)
    frame.to_csv(a.output/'data'/(typ.lower()+'_lifecycle_duration.csv'), index=False)

# Related means sharing at least one effective event; it does not mean an O2O edge.
for name, focal, partner in (
        ('eoa_market','EOA','MARKET'), ('market_eoa','MARKET','EOA'),
        ('contract_market','MARKET','CONTRACT'), ('order_contract','ORDER','CONTRACT')):
    partner_sql = 'SELECT eid,oid FROM counterparts' if partner=='CONTRACT' else f"SELECT eid,oid FROM rel WHERE typ='{partner}'"
    c.execute(f'''CREATE OR REPLACE TEMP TABLE pairs AS
        SELECT DISTINCT r.oid focal, p.oid partner FROM rel r
        JOIN ({partner_sql}) p USING(eid) WHERE r.typ='{focal}' ''')
    c.execute('''CREATE OR REPLACE TEMP TABLE degrees AS
        SELECT focal,count(*) n FROM pairs GROUP BY focal''')
    histogram(name,c.sql('SELECT n x,count(*) y FROM degrees GROUP BY n').df())
    stats['interactions'][name] = c.sql('''SELECT count(*) objects,min(n) minimum,
        max(n) maximum,median(n) median,sum(n) pairs,
        count(*) FILTER(WHERE n=1) one,count(*) FILTER(WHERE n<=2) at_most_two
        FROM degrees''').df().iloc[0].to_dict()
    stats['interactions'][name]['without_counterpart'] = c.sql(f'''
        SELECT count(DISTINCT oid) FROM rel r WHERE typ='{focal}'
        AND NOT EXISTS (SELECT 1 FROM degrees d WHERE d.focal=r.oid)''').fetchall()[0][0]

# Same selected activities and object types as the paper; absent types stay absent.
activities = ['delegate call to report','call to log initial report submission',
    'create dispute','call to create dispute','contribute to dispute','redeem dispute',
    'complete dispute','call to log market finalization','redeem as initial reporter',
    'redeem dispute crowdsourcer']
relations = c.sql('''SELECT r.eid "ocel:eid",r.oid "ocel:oid",r.typ "ocel:type",
    ev.activity "ocel:activity", ev.ts "ocel:timestamp"
    FROM rel r JOIN ev USING(eid) WHERE r.typ IN ('MARKET','ORDER')
    AND ev.activity IN (SELECT unnest(?)) ORDER BY block_no,tx_index,trace_pos,oid''', params=[activities]).df()
events = relations[['ocel:eid','ocel:activity','ocel:timestamp']].drop_duplicates('ocel:eid')
objects = relations[['ocel:oid','ocel:type']].drop_duplicates()
model = pm4py.discover_ocdfg(OCEL(events=events,objects=objects,relations=relations))
stats['dispute_objects'] = objects['ocel:type'].value_counts().to_dict()
stats['dispute_selected_activities_without_objects'] = sorted(set(activities)-set(events['ocel:activity']))
nodes = events['ocel:activity'].value_counts().rename_axis('activity').reset_index(name='events')
nodes.sort_values('activity').to_csv(a.output/'full'/'dispute_nodes.csv',index=False)
edges = []
for typ, pairs in model['edges']['event_couples'].items():
    for (src,dst), pairs_of_events in pairs.items():
        values = model['edges_performance']['event_couples'][typ][(src,dst)]
        edges.append([typ,src,dst,len(pairs_of_events),median(values),len(pairs_of_events)>=50])
pd.DataFrame(edges,columns=['type','source','target','event_couples','median_seconds','shown']).sort_values(
    ['type','source','target']).to_csv(a.output/'full'/'dispute_edges.csv',index=False)
ends = []
for side in ('start_activities','end_activities'):
    for typ, acts in model[side]['events'].items():
        for act, ids in acts.items():
            ends.append([side,typ,act,len(ids),len(ids)>=50])
pd.DataFrame(ends,columns=['side','type','activity','events','shown']).sort_values(
    ['side','type','activity']).to_csv(a.output/'full'/'dispute_endpoints.csv',index=False)
# PM4Py uses Python's randomized hash for colors. Fix the original diagram colors.
classic.ot_to_color = lambda typ: {'MARKET':'#CC7900','ORDER':'#30DD40'}[typ]
for annotation, suffix in [('frequency','freq'),('performance','perf')]:
    viz = classic.apply(model,parameters={'format':'pdf','rankdir':'TB',
        'annotation':annotation,classic.Parameters.PERFORMANCE_AGGREGATION_MEASURE:'median',
        'act_metric':'events','edge_metric':'event_couples','edge_threshold':50})
    (a.output/'figures'/('ocdfg_'+suffix+'.pdf')).write_bytes(viz.pipe(format='pdf'))

pd.DataFrame(deltas,columns=['series','bin','published','corrected','delta','shown_corrected']).to_csv(
    a.output/'histogram_deltas.csv',index=False)
(a.output/'summary.json').write_text(json.dumps(stats,indent=2,default=int)+'\n')
if a.write_paper:
    for sub in ('data','figures'):
        for source in (a.output/sub).iterdir():
            target = a.paper/sub/source.name
            if not target.is_file():
                raise FileNotFoundError(f'Refusing to add a new paper asset: {target}')
            shutil.copyfile(source,target)
print(json.dumps(stats,indent=2,default=int))
