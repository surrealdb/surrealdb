/**
[env]
namespace = true
database = true

[test]
reason = "TopK threshold pushdown EXPLAIN visibility on KV table scans: `topk_pushdown: yes` when the first ORDER BY key is a plain field path under the default comparison and the LIMIT is a literal, whatever its size and with or without TEMPFILES; the attribute is omitted where the shape is not eligible (COLLATE or NUMERIC first key, array-index path, alias resolving to an expression, parameter limit, RAND(), first key id) and where no top-K opportunity exists (no ORDER BY, no LIMIT, GROUP BY, SPLIT). Write-time `DEFINE FIELD ... VALUE` fields stay eligible (their result is stored in the record bytes); read-time `COMPUTED` fields put a projection layer between the sort and the scan, which the probe cannot cross, so the scan carries no attribute."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, arr: [3, 1], b: 2, created_at: d'2024-01-01T00:00:00Z', id: t:1, outer: { rank: 1 }, st: 200, tags: ['x'] }]"

[[test.results]]
value = "[{ a: 2, arr: [2, 5], b: 1, created_at: d'2024-01-02T00:00:00Z', id: t:2, outer: { rank: 2 }, st: 404, tags: ['y'] }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 10]
        Sort [ctx: Db] [sort_keys: created_at DESC, keep: 10]
            TableScan [ctx: Db] [table: t, direction: Forward, predicate: st INSIDE [200, 201], pre_decode_filter: yes, topk_pushdown: yes]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 10]
        Sort [ctx: Db] [sort_keys: created_at DESC, keep: 10]
            TableScan [ctx: Db] [table: t, direction: Forward, topk_pushdown: yes]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 10]
        Sort [ctx: Db] [sort_keys: created_at ASC, keep: 10]
            TableScan [ctx: Db] [table: t, direction: Forward, topk_pushdown: yes]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 10]
        Sort [ctx: Db] [sort_keys: a DESC, b ASC, keep: 10]
            TableScan [ctx: Db] [table: t, direction: Forward, topk_pushdown: yes]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 10]
        Sort [ctx: Db] [sort_keys: outer.rank DESC, keep: 10]
            TableScan [ctx: Db] [table: t, direction: Forward, topk_pushdown: yes]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 10]
        Sort [ctx: Db] [sort_keys: st DESC COLLATE, keep: 10]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 10]
        Sort [ctx: Db] [sort_keys: st DESC NUMERIC, keep: 10]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 10]
        Sort [ctx: Db] [sort_keys: arr[0] DESC, keep: 10]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: fa]
    Limit [ctx: Db] [limit: 10]
        Sort [ctx: Db] [sort_keys: math::floor(a) DESC, keep: 10]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 1001]
        Sort [ctx: Db] [sort_keys: created_at DESC, keep: 1001]
            TableScan [ctx: Db] [table: t, direction: Forward, topk_pushdown: yes]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: $lim]
        Sort [ctx: Db] [sort_keys: created_at DESC]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 10]
        Sort [ctx: Db] [sort_keys: created_at DESC, keep: 10]
            TableScan [ctx: Db] [table: t, direction: Forward, topk_pushdown: yes]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Sort [ctx: Db] [sort_keys: created_at DESC]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: t, direction: Forward, limit: 10]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 10]
        Sort [ctx: Db] [sort_keys: RAND(), keep: 10]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: t, direction: Backward, limit: 10]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 10]
        Sort [ctx: Db] [sort_keys: a DESC, keep: 10]
            Aggregate [ctx: Db] [by: a]
                TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 10]
        Sort [ctx: Db] [sort_keys: created_at DESC, keep: 10]
            Split [ctx: Db] [targets: tags]
                SelectProject [ctx: Db] [projections: *]
                    TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 10]
        Sort [ctx: Db] [sort_keys: score DESC, keep: 10]
            TableScan [ctx: Db] [table: tv, direction: Forward, topk_pushdown: yes]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 10]
        Sort [ctx: Db] [sort_keys: score DESC, keep: 10]
            ProjectLayer [ctx: Db] [columns: score = 1 @computed]
                TableScan [ctx: Db] [table: tc, direction: Forward]
''''

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `meta.rank` as `COMPUTED` fields must be top-level."
*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET st = 200, created_at = d'2024-01-01T00:00:00Z', a = 1, b = 2, arr = [3, 1], tags = ['x'], outer = { rank: 1 };
CREATE t:2 SET st = 404, created_at = d'2024-01-02T00:00:00Z', a = 2, b = 1, arr = [2, 5], tags = ['y'], outer = { rank: 2 };

-- Canonical shape: WHERE probe and TopK threshold probe both active on the scan.
EXPLAIN SELECT * FROM t WITH NOINDEX WHERE st IN [200, 201] ORDER BY created_at DESC LIMIT 10;
-- Plain ORDER BY + LIMIT without WHERE.
EXPLAIN SELECT * FROM t ORDER BY created_at DESC LIMIT 10;
-- ASC is equally eligible (threshold rejection is direction-aware).
EXPLAIN SELECT * FROM t ORDER BY created_at ASC LIMIT 10;
-- Multi-key ORDER BY: only the first key is probed; still eligible.
EXPLAIN SELECT * FROM t ORDER BY a DESC, b ASC LIMIT 10;
-- Nested field chain as the first key.
EXPLAIN SELECT * FROM t ORDER BY outer.rank DESC LIMIT 10;

-- COLLATE / NUMERIC comparison modes are not replicated on wire bytes.
EXPLAIN SELECT * FROM t ORDER BY st COLLATE DESC LIMIT 10;
EXPLAIN SELECT * FROM t ORDER BY st NUMERIC DESC LIMIT 10;
-- Array-index path parts cannot be walked on wire bytes.
EXPLAIN SELECT * FROM t ORDER BY arr[0] DESC LIMIT 10;
-- Alias resolving to an expression is a synthetic post-decode sort key.
EXPLAIN SELECT math::floor(a) AS fa FROM t ORDER BY fa DESC LIMIT 10;

-- A large literal limit is as eligible as a small one.
EXPLAIN SELECT * FROM t ORDER BY created_at DESC LIMIT 1001;
-- Parameter limits cannot bound the heap at plan time.
LET $lim = 10;
EXPLAIN SELECT * FROM t ORDER BY created_at DESC LIMIT $lim;
-- TEMPFILES spills only a sort with no limit; a bounded sort keeps its heap.
EXPLAIN SELECT * FROM t ORDER BY created_at DESC LIMIT 10 TEMPFILES;

-- Attribute omitted: no top-K opportunity exists at all.
EXPLAIN SELECT * FROM t ORDER BY created_at DESC;
EXPLAIN SELECT * FROM t LIMIT 10;
EXPLAIN SELECT * FROM t ORDER BY RAND() LIMIT 10;
EXPLAIN SELECT * FROM t ORDER BY id DESC LIMIT 10;
EXPLAIN SELECT a FROM t GROUP BY a ORDER BY a DESC LIMIT 10;
EXPLAIN SELECT * FROM t SPLIT tags ORDER BY created_at DESC LIMIT 10;

-- Write-time VALUE fields are materialised into the stored bytes — eligible.
DEFINE TABLE tv SCHEMALESS;
DEFINE FIELD score ON tv VALUE 1;
EXPLAIN SELECT * FROM tv ORDER BY score DESC LIMIT 10;
-- Read-time COMPUTED fields are derived by a projection layer above the scan;
-- the sort cannot cross it, so the scan carries no probe.
DEFINE TABLE tc SCHEMALESS;
DEFINE FIELD score ON tc COMPUTED 1;
EXPLAIN SELECT * FROM tc ORDER BY score DESC LIMIT 10;
-- Nested COMPUTED definitions are rejected at DDL time — this pinned error
-- is the invariant that makes nested read-time fields unreachable. If it
-- ever lifts, the eligibility check already blocks computed names extending
-- the ORDER BY root with `.`/`[` (see field_state_blocks_raw_read), and this
-- test must grow EXPLAIN coverage for that shape.
DEFINE TABLE tn SCHEMALESS;
DEFINE FIELD meta.rank ON tn COMPUTED 1;
