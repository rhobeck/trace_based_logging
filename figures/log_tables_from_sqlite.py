"""Writes the relational tables the figure scripts read from the OCEL 2.0 SQLite
file of the event log: events, objects, e2o (event-to-object relations), o2o
(object-to-object relations) and object_changes (attribute histories), as parquet.

The tables hold all events, objects and relations of the log, with the columns
the figure scripts use. Of the event attributes these are the block number, the
transaction index, the position in the transaction trace and the revert flag;
the other event attributes stay in the SQLite file.

Requires duckdb with its SQLite extension, which duckdb downloads on first use.

usage: log_tables_from_sqlite.py <event log .sqlite> <out folder>
"""
import sys
from pathlib import Path

import duckdb

db, out = sys.argv[1], Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)

c = duckdb.connect()
# every SQLite column is read as text and cast below
c.execute("INSTALL sqlite; LOAD sqlite; SET sqlite_all_varchar=true")
c.execute(f"ATTACH '{db}' AS log (TYPE sqlite, READ_ONLY)")


def literal(text):
    return "'" + text.replace("'", "''") + "'"


def columns(table):
    return {row[0] for row in c.sql(f'DESCRIBE log."{table}"').fetchall()}


def write(name, query):
    c.execute(f"COPY ({query}) TO '{out / name}.parquet' (FORMAT parquet)")
    rows = c.sql(f"SELECT count(*) FROM read_parquet('{out / name}.parquet')").fetchall()[0][0]
    print(f"{name}: {rows:,} rows")


# OCEL 2.0 stores one table per event type and per object type; the map tables
# give the table name of each type.
event_types = c.sql('SELECT ocel_type, ocel_type_map FROM log.event_map_type').fetchall()
object_types = c.sql('SELECT ocel_type, ocel_type_map FROM log.object_map_type').fetchall()

write('events', ' UNION ALL '.join(f'''
    SELECT ocel_id "ocel:eid", {literal(activity)} "ocel:activity",
           ocel_time::TIMESTAMP "ocel:timestamp", "blockNumber"::BIGINT "blockNumber",
           "transactionIndex"::BIGINT "transactionIndex", "tracePos"::BIGINT "tracePos",
           (flag_reverted::INT <> 0) flag_reverted
    FROM log."event_{table}"''' for activity, table in event_types))

# An object table holds one row per object and, for a type with an attribute
# history, one further row per change (ocel_changed_field is set).
objects, changes = [], []
for object_type, table in object_types:
    cols = columns(f'object_{table}')
    history = 'ocel_changed_field' in cols
    objects.append(f'''
        SELECT DISTINCT {'category' if 'category' in cols else 'NULL'} category,
               ocel_id "ocel:oid", {literal(object_type)} "ocel:type",
               {'account_type' if 'account_type' in cols else 'NULL'} account_type
        FROM log."object_{table}" {'WHERE ocel_changed_field IS NULL' if history else ''}''')
    if history:
        changes.append(f'''
            SELECT ocel_id "ocel:oid", {literal(object_type)} "ocel:type",
                   ocel_time::TIMESTAMP "ocel:timestamp", ocel_changed_field "ocel:field", state
            FROM log."object_{table}" WHERE ocel_changed_field IS NOT NULL''')
write('objects', ' UNION ALL '.join(objects))
write('object_changes', ' UNION ALL '.join(changes))

write('e2o', '''
    SELECT r.ocel_event_id "ocel:eid", r.ocel_object_id "ocel:oid", o.ocel_type "ocel:type",
           r.ocel_qualifier "ocel:qualifier"
    FROM log.event_object r JOIN log.object o ON o.ocel_id = r.ocel_object_id''')
write('o2o', '''
    SELECT ocel_source_id "ocel:oid", ocel_qualifier "ocel:qualifier", ocel_target_id "ocel:oid_2"
    FROM log.object_object''')
