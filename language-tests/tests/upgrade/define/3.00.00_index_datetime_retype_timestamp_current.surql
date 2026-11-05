/**
[env]
imports = ["upgrade/define/3.00.00_index_datetime_retype_timestamp_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
The plans the reads in 3.00.00_index_datetime_retype_timestamp.surql are
answered by: each read is the index scan's own access, equality before the
backfill over the entries an old binary wrote, and range and equality after it. Without
these an index the new binary cannot read would fall back to a table scan and
return the same rows.

A range over the old writer's non-unique index sits under `DistinctRecords`:
an index the old writer built records no stored shape, so the planner cannot
rule out an array in its column and lists each record once.

`EXPLAIN` output is not stable across the 3.x line, so the upgrade target is
the current checkout; the seed axis sends every released version straight to
it."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: ti, index: ti_ts, access: = d'2026-10-27T12:00:00Z', direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: tu, index: tu_ts, access: = d'2026-10-27T12:00:00Z', direction: Forward]
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    DistinctRecords [ctx: Db]
        IndexScan [ctx: Db] [table: ti, index: ti_ts, access: >=ts'2026-10-27T12:00:00', direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: ti, index: ti_ts, access: = ts'2026-10-27T12:00:00', direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: tu, index: tu_ts, access: = ts'2026-10-27T12:00:00', direction: Forward]
"'''
*/
DEFINE FIELD OVERWRITE ts ON ti TYPE timestamp;
DEFINE FIELD OVERWRITE ts ON tu TYPE timestamp;
-- Over the entries the old writer laid out.
EXPLAIN SELECT VALUE id FROM ti WHERE ts = d'2026-10-27T12:00:00Z';
EXPLAIN SELECT VALUE id FROM tu WHERE ts = d'2026-10-27T12:00:00Z';
UPDATE ti SET ts = <timestamp> ts RETURN NONE;
UPDATE tu SET ts = <timestamp> ts RETURN NONE;
-- Over the entries the backfill wrote.
EXPLAIN SELECT VALUE id FROM ti WHERE ts >= ts'2026-10-27T12:00:00';
EXPLAIN SELECT VALUE id FROM ti WHERE ts = ts'2026-10-27T12:00:00';
EXPLAIN SELECT VALUE id FROM tu WHERE ts = ts'2026-10-27T12:00:00';
