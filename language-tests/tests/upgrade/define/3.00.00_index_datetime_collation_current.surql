/**
[env]
imports = ["upgrade/define/3.00.00_index_datetime_collation_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
# Every released binary stamps an index below format version 4, so the source
# is bounded to those, which also keeps the ordinary runner away. `EXPLAIN`
# output is not stable across the 3.x line, so the target is the current
# checkout; the seed axis sends every released version straight to it.
importing-version = "<3.4.0-0"
version = ">=3.4.0-a"
reason = """
The plans the reads in 3.00.00_index_datetime_collation.surql are answered by,
and the writes this checkout makes to the index after the hop. The index keeps
the text spelling its writer gave it, and every key this checkout builds for it
is spelled the same way: an update and a delete remove the entries the old
binary wrote, so a seek on a value no record holds any more finds nothing, a
row written now is found by a seek, and a read over every entry names each
record once. A range open at its upper end is read from the table, since the
index recorded nothing about the value shapes it stores, so that last read no
longer goes through the index; the equality seeks above carry the check."""

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: ts_test, index: idx_ts, access: = d'2026-08-28T09:00:00Z', direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: ts]
    Sort [ctx: Db] [sort_keys: ts ASC]
        TableScan [ctx: Db] [table: ts_test, direction: Forward, predicate: ts >= d'2026-08-28T09:00:00Z', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[ts_test:0]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[ts_test:5]"

[[test.results]]
value = "[ts_test:0, ts_test:2, ts_test:3, ts_test:4, ts_test:5]"

*/
-- The equality seek and the range are each the index scan's own access.
EXPLAIN SELECT id FROM ts_test WITH INDEX idx_ts WHERE ts = d'2026-08-28T09:00:00Z';
EXPLAIN SELECT VALUE ts FROM ts_test WITH INDEX idx_ts WHERE ts >= d'2026-08-28T09:00:00Z' ORDER BY ts ASC;

-- Move a whole-second row to another instant, and delete a fractional one.
UPDATE ts_test:0 SET ts = d'2026-08-28T09:00:05Z' RETURN NONE;
DELETE ts_test:1;

-- Neither old value is held by any record any more, so a seek finds nothing.
SELECT VALUE id FROM ts_test WITH INDEX idx_ts WHERE ts = d'2026-08-28T09:00:00Z';
SELECT VALUE id FROM ts_test WITH INDEX idx_ts WHERE ts = d'2026-08-28T09:00:00.152Z';

-- The moved row is found at its new value.
SELECT VALUE id FROM ts_test WITH INDEX idx_ts WHERE ts = d'2026-08-28T09:00:05Z';

-- A row written now is found by a seek on its value.
CREATE ts_test:5 SET ts = d'2026-08-28T09:00:00.500Z' RETURN NONE;
SELECT VALUE id FROM ts_test WITH INDEX idx_ts WHERE ts = d'2026-08-28T09:00:00.500Z';

-- Every entry, in index order, names each record once.
SELECT VALUE id FROM ts_test WITH INDEX idx_ts WHERE ts >= d'2026-01-01T00:00:00Z';
