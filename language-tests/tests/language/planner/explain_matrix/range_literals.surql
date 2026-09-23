/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = "Range literals as index access paths. A range literal used as the operand of `IN`/`INSIDE` is exactly a BTree range and is read as one, each spelling carrying its own pair of bounds: `lo..hi` seeks `>=lo <hi`, `lo..=hi` seeks `>=lo <=hi`, `lo>..hi` seeks `>lo <hi` and `lo>..=hi` seeks `>lo <=hi`. All four are covered over a standard index and over a UNIQUE one, which keys one record per value rather than fanning out and so constructs its bounds on a separate seek path. The fixture places a row exactly ON the low bound and another exactly ON the high bound, which is what makes the four spellings distinguishable by rows rather than only by rendering: an off-by-one in either direction moves a row in or out of the answer instead of merely mislabelling the plan. Every plan assertion is therefore paired with the same query under `WITH NOINDEX`, which forces a table scan and is the oracle -- EXPLAIN shows which operator ran, not whether it returned the right rows. The index-only count path is pinned for all four spellings too: `count() ... GROUP ALL` is served by IndexCountScan, which derives the same bounds without fetching records, so it can disagree with the row path. Beyond the four spellings the file pins: a range on an unindexed column, which must stay a TableScan, because a column acquiring an index scan it has no index for is exactly the silent regression this suite exists to catch; a range against a compound index's LEADING column, where a range is a key bound rather than a prefix component and so terminates the prefix -- the residual is asserted both ways, the range alone leaving its condition as the scan's own `predicate:` while a trailing equality on the second column becomes a separate `Filter` above a scan carrying no `predicate:` at all, with a fixture row whose leading column lies inside the range while its second column fails the residual, so folding that residual into the key bounds would seek (10, 2) to (20, 2), return it, and break the contract on rows rather than only on the rendered plan; ranges of strings and of datetimes, since bounds are values rather than numbers; a reversed range, whose lower bound sits above its upper one and which therefore addresses no keys, pinned for rows and for count because an index-only count walking it the wrong way would report the complement; the operands swapped so the range is the LEFT operand (`10..=20 CONTAINS n`), a different operator with no index arm; and `INSIDE` as the long spelling of `IN`, which plans identically. Last, `v = 10..=20` compares the field to a range VALUE and must NOT become a range scan: it is the exact-key seek `access: = 10..=20`. Its table stores a row holding that very range, a row holding the neighbouring spelling `10..20`, and a row holding 15, which lies inside the range but is not equal to it -- so lowering `=` into a range scan would return the 15 and miss the range, and matching the range loosely would also return the other spelling. The `IN` query over the same column is asserted beside it so the two readings of one literal sit together. `comparison_operators.surql` already pins the two basic `IN 30..40` / `IN 30..=40` cells against its own fixture; this file is the rest of the axis. `EXPLAIN` is rejected by the compute-only engine, so the planner axis is narrowed to the two streaming strategies; they plan identically by contract, so both are pinned rather than one. Each plan is asserted on the query without `ORDER BY` and the rows on the same query with `ORDER BY id`, which makes the row comparison deterministic without putting a sort wrapper into every plan."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c1: 5, c2: 1, d: d'2021-01-01T00:00:00Z', id: rng:r1, n: 5, plain: 5, s: 'aa', u: 5 }]"

[[test.results]]
value = "[{ c1: 10, c2: 1, d: d'2022-01-01T00:00:00Z', id: rng:r2, n: 10, plain: 10, s: 'bb', u: 10 }]"

[[test.results]]
value = "[{ c1: 15, c2: 2, d: d'2023-01-01T00:00:00Z', id: rng:r3, n: 15, plain: 15, s: 'cc', u: 15 }]"

[[test.results]]
value = "[{ c1: 20, c2: 2, d: d'2024-01-01T00:00:00Z', id: rng:r4, n: 20, plain: 20, s: 'dd', u: 20 }]"

[[test.results]]
value = "[{ c1: 25, c2: 3, d: d'2025-01-01T00:00:00Z', id: rng:r5, n: 25, plain: 25, s: 'ee', u: 25 }]"

[[test.results]]
value = "[{ c1: 15, c2: 1, d: d'2030-01-01T00:00:00Z', id: rng:r6, n: 100, plain: 100, s: 'zz', u: 100 }]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_n, access: >=10 <20, direction: Forward, predicate: n INSIDE 10 .. 20]
''''

[[test.results]]
value = "[rng:r2, rng:r3]"

[[test.results]]
value = "[rng:r2, rng:r3]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_n, access: >=10 <=20, direction: Forward, predicate: n INSIDE 10 ..= 20]
''''

[[test.results]]
value = "[rng:r2, rng:r3, rng:r4]"

[[test.results]]
value = "[rng:r2, rng:r3, rng:r4]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_n, access: >10 <20, direction: Forward, predicate: n INSIDE 10 >.. 20]
''''

[[test.results]]
value = "[rng:r3]"

[[test.results]]
value = "[rng:r3]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_n, access: >10 <=20, direction: Forward, predicate: n INSIDE 10 >..= 20]
''''

[[test.results]]
value = "[rng:r3, rng:r4]"

[[test.results]]
value = "[rng:r3, rng:r4]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_u, access: >=10 <20, direction: Forward, predicate: u INSIDE 10 .. 20]
''''

[[test.results]]
value = "[rng:r2, rng:r3]"

[[test.results]]
value = "[rng:r2, rng:r3]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_u, access: >=10 <=20, direction: Forward, predicate: u INSIDE 10 ..= 20]
''''

[[test.results]]
value = "[rng:r2, rng:r3, rng:r4]"

[[test.results]]
value = "[rng:r2, rng:r3, rng:r4]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_u, access: >10 <20, direction: Forward, predicate: u INSIDE 10 >.. 20]
''''

[[test.results]]
value = "[rng:r3]"

[[test.results]]
value = "[rng:r3]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_u, access: >10 <=20, direction: Forward, predicate: u INSIDE 10 >..= 20]
''''

[[test.results]]
value = "[rng:r3, rng:r4]"

[[test.results]]
value = "[rng:r3, rng:r4]"

[[test.results]]
value = '''
'IndexCountScan [ctx: Db] [source: rng, condition: n INSIDE 10 .. 20]
''''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
'IndexCountScan [ctx: Db] [source: rng, condition: n INSIDE 10 ..= 20]
''''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
'IndexCountScan [ctx: Db] [source: rng, condition: n INSIDE 10 >.. 20]
''''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
'IndexCountScan [ctx: Db] [source: rng, condition: n INSIDE 10 >..= 20]
''''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: rng, direction: Forward, predicate: plain INSIDE 10 ..= 20, pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = "[rng:r2, rng:r3, rng:r4]"

[[test.results]]
value = "[rng:r2, rng:r3, rng:r4]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_c, access: >=10 <=20, direction: Forward, predicate: c1 INSIDE 10 ..= 20]
''''

[[test.results]]
value = "[rng:r2, rng:r3, rng:r4, rng:r6]"

[[test.results]]
value = "[rng:r2, rng:r3, rng:r4, rng:r6]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: c2 = 2]
        IndexScan [ctx: Db] [index: idx_c, access: >=10 <=20, direction: Forward]
''''

[[test.results]]
value = "[rng:r3, rng:r4]"

[[test.results]]
value = "[rng:r3, rng:r4]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_s, access: >='bb' <'dd', direction: Forward, predicate: s INSIDE 'bb' .. 'dd']
"'''

[[test.results]]
value = "[rng:r2, rng:r3]"

[[test.results]]
value = "[rng:r2, rng:r3]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_s, access: >='bb' <='dd', direction: Forward, predicate: s INSIDE 'bb' ..= 'dd']
"'''

[[test.results]]
value = "[rng:r2, rng:r3, rng:r4]"

[[test.results]]
value = "[rng:r2, rng:r3, rng:r4]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_s, access: >'bb' <'dd', direction: Forward, predicate: s INSIDE 'bb' >.. 'dd']
"'''

[[test.results]]
value = "[rng:r3]"

[[test.results]]
value = "[rng:r3]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_s, access: >'bb' <='dd', direction: Forward, predicate: s INSIDE 'bb' >..= 'dd']
"'''

[[test.results]]
value = "[rng:r3, rng:r4]"

[[test.results]]
value = "[rng:r3, rng:r4]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_d, access: >=d'2022-01-01T00:00:00Z' <d'2024-01-01T00:00:00Z', direction: Forward, predicate: d INSIDE d'2022-01-01T00:00:00Z' .. d'2024-01-01T00:00:00Z']
"'''

[[test.results]]
value = "[rng:r2, rng:r3]"

[[test.results]]
value = "[rng:r2, rng:r3]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_d, access: >=d'2022-01-01T00:00:00Z' <=d'2024-01-01T00:00:00Z', direction: Forward, predicate: d INSIDE d'2022-01-01T00:00:00Z' ..= d'2024-01-01T00:00:00Z']
"'''

[[test.results]]
value = "[rng:r2, rng:r3, rng:r4]"

[[test.results]]
value = "[rng:r2, rng:r3, rng:r4]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_d, access: >d'2022-01-01T00:00:00Z' <d'2024-01-01T00:00:00Z', direction: Forward, predicate: d INSIDE d'2022-01-01T00:00:00Z' >.. d'2024-01-01T00:00:00Z']
"'''

[[test.results]]
value = "[rng:r3]"

[[test.results]]
value = "[rng:r3]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_d, access: >d'2022-01-01T00:00:00Z' <=d'2024-01-01T00:00:00Z', direction: Forward, predicate: d INSIDE d'2022-01-01T00:00:00Z' >..= d'2024-01-01T00:00:00Z']
"'''

[[test.results]]
value = "[rng:r3, rng:r4]"

[[test.results]]
value = "[rng:r3, rng:r4]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_n, access: >=20 <10, direction: Forward, predicate: n INSIDE 20 .. 10]
''''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
'IndexCountScan [ctx: Db] [source: rng, condition: n INSIDE 20 .. 10]
''''

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: rng, direction: Forward, predicate: 10 ..= 20 CONTAINS n, pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = "[rng:r2, rng:r3, rng:r4]"

[[test.results]]
value = "[rng:r2, rng:r3, rng:r4]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_n, access: >=10 <=20, direction: Forward, predicate: n INSIDE 10 ..= 20]
''''

[[test.results]]
value = "[rng:r2, rng:r3, rng:r4]"

[[test.results]]
value = "[rng:r2, rng:r3, rng:r4]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: rv:x, v: 10..=20 }]"

[[test.results]]
value = "[{ id: rv:y, v: 15 }]"

[[test.results]]
value = "[{ id: rv:z, v: 10..20 }]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_v, access: = 10..=20, direction: Forward, predicate: v = 10 ..= 20]
''''

[[test.results]]
value = "[rv:x]"

[[test.results]]
value = "[rv:x]"

[[test.results]]
value = '''
'IndexCountScan [ctx: Db] [source: rv, condition: v = 10 ..= 20]
''''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: idx_v, access: >=10 <=20, direction: Forward, predicate: v INSIDE 10 ..= 20]
''''

[[test.results]]
value = "[rv:y]"

[[test.results]]
value = "[rv:y]"

[[test.results]]
value = '''
'IndexCountScan [ctx: Db] [source: rv, condition: v INSIDE 10 ..= 20]
''''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"
*/
-- A range literal used as the operand of `IN`/`INSIDE` is exactly a BTree
-- range and is read as one, each spelling carrying its own pair of bounds:
-- `lo..hi` seeks `>=lo <hi`, `lo..=hi` seeks `>=lo <=hi`, `lo>..hi` seeks
-- `>lo <hi` and `lo>..=hi` seeks `>lo <=hi`. The fixture below puts a row
-- exactly ON each bound, so confusing any spelling for its neighbour changes
-- the row set rather than only the rendered plan.

-- ==========================================================================
-- Fixture. `n` carries a standard index, `u` the same values under a UNIQUE
-- index, `s` strings and `d` datetimes, `plain` is deliberately unindexed,
-- and `c1, c2` is a compound index whose leading column is `c1`. Every field
-- is typed, so no indexed column here can hold an array and the fan-out key
-- model is out of scope; the array arms live in array_fanout_oracle.surql.
-- Values are 5/10/15/20/25 so that a range of 10..20 has a row on the low
-- bound (r2), a row strictly inside (r3), a row on the high bound (r4) and a
-- row beyond each end (r1, r5).
-- r6 lies outside every range asserted below on every column but c1, where
-- it sits strictly inside the range while its c2 fails the residual, which
-- is what makes the compound arm's rows discriminating.
-- ==========================================================================
DEFINE TABLE rng SCHEMAFULL;
DEFINE FIELD n ON rng TYPE int;
DEFINE FIELD u ON rng TYPE int;
DEFINE FIELD s ON rng TYPE string;
DEFINE FIELD d ON rng TYPE datetime;
DEFINE FIELD plain ON rng TYPE int;
DEFINE FIELD c1 ON rng TYPE int;
DEFINE FIELD c2 ON rng TYPE int;
DEFINE INDEX idx_n ON rng FIELDS n;
DEFINE INDEX idx_u ON rng FIELDS u UNIQUE;
DEFINE INDEX idx_s ON rng FIELDS s;
DEFINE INDEX idx_d ON rng FIELDS d;
DEFINE INDEX idx_c ON rng FIELDS c1, c2;

CREATE rng:r1 SET n = 5,  u = 5,  s = 'aa', d = d'2021-01-01T00:00:00Z', plain = 5,  c1 = 5,  c2 = 1;
CREATE rng:r2 SET n = 10, u = 10, s = 'bb', d = d'2022-01-01T00:00:00Z', plain = 10, c1 = 10, c2 = 1;
CREATE rng:r3 SET n = 15, u = 15, s = 'cc', d = d'2023-01-01T00:00:00Z', plain = 15, c1 = 15, c2 = 2;
CREATE rng:r4 SET n = 20, u = 20, s = 'dd', d = d'2024-01-01T00:00:00Z', plain = 20, c1 = 20, c2 = 2;
CREATE rng:r5 SET n = 25, u = 25, s = 'ee', d = d'2025-01-01T00:00:00Z', plain = 25, c1 = 25, c2 = 3;
CREATE rng:r6 SET n = 100, u = 100, s = 'zz', d = d'2030-01-01T00:00:00Z', plain = 100, c1 = 15, c2 = 1;

-- ==========================================================================
-- Group A: the four spellings over a standard index on an integer column.
-- The four row sets are pairwise distinct because r2 sits on the low bound
-- and r4 on the high one, so each `access:` is checked by its rows and not
-- only by its rendering.
-- ==========================================================================
EXPLAIN SELECT VALUE id FROM rng WHERE n IN 10..20;
SELECT VALUE id FROM rng WHERE n IN 10..20 ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE n IN 10..20 ORDER BY id;

EXPLAIN SELECT VALUE id FROM rng WHERE n IN 10..=20;
SELECT VALUE id FROM rng WHERE n IN 10..=20 ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE n IN 10..=20 ORDER BY id;

EXPLAIN SELECT VALUE id FROM rng WHERE n IN 10>..20;
SELECT VALUE id FROM rng WHERE n IN 10>..20 ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE n IN 10>..20 ORDER BY id;

EXPLAIN SELECT VALUE id FROM rng WHERE n IN 10>..=20;
SELECT VALUE id FROM rng WHERE n IN 10>..=20 ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE n IN 10>..=20 ORDER BY id;

-- ==========================================================================
-- Group B: the same four spellings over a UNIQUE index. A unique index keys
-- one record per value rather than fanning out, so it is a separate seek
-- path and carries its own bound construction.
-- ==========================================================================
EXPLAIN SELECT VALUE id FROM rng WHERE u IN 10..20;
SELECT VALUE id FROM rng WHERE u IN 10..20 ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE u IN 10..20 ORDER BY id;

EXPLAIN SELECT VALUE id FROM rng WHERE u IN 10..=20;
SELECT VALUE id FROM rng WHERE u IN 10..=20 ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE u IN 10..=20 ORDER BY id;

EXPLAIN SELECT VALUE id FROM rng WHERE u IN 10>..20;
SELECT VALUE id FROM rng WHERE u IN 10>..20 ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE u IN 10>..20 ORDER BY id;

EXPLAIN SELECT VALUE id FROM rng WHERE u IN 10>..=20;
SELECT VALUE id FROM rng WHERE u IN 10>..=20 ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE u IN 10>..=20 ORDER BY id;

-- ==========================================================================
-- Group C: the index-only count path. `count() … GROUP ALL` over a range is
-- served by IndexCountScan, which counts index entries without fetching the
-- records, so it derives the same bounds through a second code path. Each
-- count is paired with the NOINDEX oracle that fetches and counts rows.
-- ==========================================================================
EXPLAIN SELECT count() FROM rng WHERE n IN 10..20 GROUP ALL;
SELECT count() FROM rng WHERE n IN 10..20 GROUP ALL;
SELECT count() FROM rng WITH NOINDEX WHERE n IN 10..20 GROUP ALL;

EXPLAIN SELECT count() FROM rng WHERE n IN 10..=20 GROUP ALL;
SELECT count() FROM rng WHERE n IN 10..=20 GROUP ALL;
SELECT count() FROM rng WITH NOINDEX WHERE n IN 10..=20 GROUP ALL;

EXPLAIN SELECT count() FROM rng WHERE n IN 10>..20 GROUP ALL;
SELECT count() FROM rng WHERE n IN 10>..20 GROUP ALL;
SELECT count() FROM rng WITH NOINDEX WHERE n IN 10>..20 GROUP ALL;

EXPLAIN SELECT count() FROM rng WHERE n IN 10>..=20 GROUP ALL;
SELECT count() FROM rng WHERE n IN 10>..=20 GROUP ALL;
SELECT count() FROM rng WITH NOINDEX WHERE n IN 10>..=20 GROUP ALL;

-- ==========================================================================
-- Group D: a range literal on a column with no index. There is nothing to
-- seek, so the shape falls to a TableScan carrying the whole condition. It
-- is pinned because a column silently acquiring an index scan it has no
-- index for is the regression this file exists to catch.
-- ==========================================================================
EXPLAIN SELECT VALUE id FROM rng WHERE plain IN 10..=20;
SELECT VALUE id FROM rng WHERE plain IN 10..=20 ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE plain IN 10..=20 ORDER BY id;

-- ==========================================================================
-- Group E: a range literal against a compound index's LEADING column. A
-- range is a key bound rather than a prefix component, so it terminates the
-- prefix: the scan takes the bound and a condition on the following column
-- cannot be seeked. The two shapes below pin where the residual goes — the
-- range alone leaves its condition as the scan's own `predicate:`, whereas
-- the trailing equality becomes a separate `Filter` above a scan that
-- carries no `predicate:` at all.
-- r6 is what makes the second shape's rows load-bearing as well as its
-- plan: its c1 lies strictly inside the range while its c2 is 1, so a
-- residual folded into the key bounds would seek (10, 2) to (20, 2),
-- pick r6 up and change the answer rather than only the rendering.
-- ==========================================================================
EXPLAIN SELECT VALUE id FROM rng WHERE c1 IN 10..=20;
SELECT VALUE id FROM rng WHERE c1 IN 10..=20 ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE c1 IN 10..=20 ORDER BY id;

EXPLAIN SELECT VALUE id FROM rng WHERE c1 IN 10..=20 AND c2 = 2;
SELECT VALUE id FROM rng WHERE c1 IN 10..=20 AND c2 = 2 ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE c1 IN 10..=20 AND c2 = 2 ORDER BY id;

-- ==========================================================================
-- Group F: ranges of strings. Bounds are values, not numbers, so the same
-- four spellings must reach the string index with the string bounds intact.
-- 'bb' and 'dd' are both present, so the endpoint arms discriminate.
-- ==========================================================================
EXPLAIN SELECT VALUE id FROM rng WHERE s IN 'bb'..'dd';
SELECT VALUE id FROM rng WHERE s IN 'bb'..'dd' ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE s IN 'bb'..'dd' ORDER BY id;

EXPLAIN SELECT VALUE id FROM rng WHERE s IN 'bb'..='dd';
SELECT VALUE id FROM rng WHERE s IN 'bb'..='dd' ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE s IN 'bb'..='dd' ORDER BY id;

EXPLAIN SELECT VALUE id FROM rng WHERE s IN 'bb'>..'dd';
SELECT VALUE id FROM rng WHERE s IN 'bb'>..'dd' ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE s IN 'bb'>..'dd' ORDER BY id;

EXPLAIN SELECT VALUE id FROM rng WHERE s IN 'bb'>..='dd';
SELECT VALUE id FROM rng WHERE s IN 'bb'>..='dd' ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE s IN 'bb'>..='dd' ORDER BY id;

-- ==========================================================================
-- Group G: ranges of datetimes, over the same four spellings.
-- ==========================================================================
EXPLAIN SELECT VALUE id FROM rng WHERE d IN d'2022-01-01T00:00:00Z'..d'2024-01-01T00:00:00Z';
SELECT VALUE id FROM rng WHERE d IN d'2022-01-01T00:00:00Z'..d'2024-01-01T00:00:00Z' ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE d IN d'2022-01-01T00:00:00Z'..d'2024-01-01T00:00:00Z' ORDER BY id;

EXPLAIN SELECT VALUE id FROM rng WHERE d IN d'2022-01-01T00:00:00Z'..=d'2024-01-01T00:00:00Z';
SELECT VALUE id FROM rng WHERE d IN d'2022-01-01T00:00:00Z'..=d'2024-01-01T00:00:00Z' ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE d IN d'2022-01-01T00:00:00Z'..=d'2024-01-01T00:00:00Z' ORDER BY id;

EXPLAIN SELECT VALUE id FROM rng WHERE d IN d'2022-01-01T00:00:00Z'>..d'2024-01-01T00:00:00Z';
SELECT VALUE id FROM rng WHERE d IN d'2022-01-01T00:00:00Z'>..d'2024-01-01T00:00:00Z' ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE d IN d'2022-01-01T00:00:00Z'>..d'2024-01-01T00:00:00Z' ORDER BY id;

EXPLAIN SELECT VALUE id FROM rng WHERE d IN d'2022-01-01T00:00:00Z'>..=d'2024-01-01T00:00:00Z';
SELECT VALUE id FROM rng WHERE d IN d'2022-01-01T00:00:00Z'>..=d'2024-01-01T00:00:00Z' ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE d IN d'2022-01-01T00:00:00Z'>..=d'2024-01-01T00:00:00Z' ORDER BY id;

-- ==========================================================================
-- Group H: a reversed range. The bounds are lowered as written, giving a
-- lower bound above the upper one, which addresses no keys and yields no
-- rows -- the same answer the oracle reaches by testing every record. The
-- count path is pinned alongside, because an index-only count that walked
-- such a range in the wrong direction would report the complement.
-- ==========================================================================
EXPLAIN SELECT VALUE id FROM rng WHERE n IN 20..10;
SELECT VALUE id FROM rng WHERE n IN 20..10 ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE n IN 20..10 ORDER BY id;

EXPLAIN SELECT count() FROM rng WHERE n IN 20..10 GROUP ALL;
SELECT count() FROM rng WHERE n IN 20..10 GROUP ALL;
SELECT count() FROM rng WITH NOINDEX WHERE n IN 20..10 GROUP ALL;

-- ==========================================================================
-- Group I: the operand positions swapped, so the range is the LEFT operand
-- and the indexed field the right one. Containment with the collection on
-- the left is a different operator from `IN`, and it has no index arm: the
-- shape reaches a TableScan and the filter cannot even run before the record
-- is decoded. The rows still have to match the oracle.
-- ==========================================================================
EXPLAIN SELECT VALUE id FROM rng WHERE 10..=20 CONTAINS n;
SELECT VALUE id FROM rng WHERE 10..=20 CONTAINS n ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE 10..=20 CONTAINS n ORDER BY id;

-- ==========================================================================
-- Group J: `INSIDE` is the long spelling of `IN` and plans identically down
-- to the rendered predicate.
-- ==========================================================================
EXPLAIN SELECT VALUE id FROM rng WHERE n INSIDE 10..=20;
SELECT VALUE id FROM rng WHERE n INSIDE 10..=20 ORDER BY id;
SELECT VALUE id FROM rng WITH NOINDEX WHERE n INSIDE 10..=20 ORDER BY id;

-- ==========================================================================
-- Group K: a range literal on the right of `=` is a range VALUE being
-- compared for equality, not a range to scan. The seek is the exact-key seek
-- `access: = 10..=20`, and it must find the record whose field holds that
-- very range and nothing else. The fixture makes the two readings
-- distinguishable in both directions: rv:x stores the range 10..=20, rv:z
-- stores the neighbouring spelling 10..20, and rv:y stores 15, which lies
-- inside the range but is not equal to it. Lowering `=` into a range scan
-- would return rv:y and miss rv:x; matching the range loosely would also
-- return rv:z. The `IN` query on the same column is asserted directly below
-- so that the two readings of the same literal sit side by side.
-- ==========================================================================
DEFINE TABLE rv SCHEMALESS;
DEFINE INDEX idx_v ON rv FIELDS v;
CREATE rv:x SET v = 10..=20;
CREATE rv:y SET v = 15;
CREATE rv:z SET v = 10..20;

EXPLAIN SELECT VALUE id FROM rv WHERE v = 10..=20;
SELECT VALUE id FROM rv WHERE v = 10..=20 ORDER BY id;
SELECT VALUE id FROM rv WITH NOINDEX WHERE v = 10..=20 ORDER BY id;

EXPLAIN SELECT count() FROM rv WHERE v = 10..=20 GROUP ALL;
SELECT count() FROM rv WHERE v = 10..=20 GROUP ALL;
SELECT count() FROM rv WITH NOINDEX WHERE v = 10..=20 GROUP ALL;

EXPLAIN SELECT VALUE id FROM rv WHERE v IN 10..=20;
SELECT VALUE id FROM rv WHERE v IN 10..=20 ORDER BY id;
SELECT VALUE id FROM rv WITH NOINDEX WHERE v IN 10..=20 ORDER BY id;

EXPLAIN SELECT count() FROM rv WHERE v IN 10..=20 GROUP ALL;
SELECT count() FROM rv WHERE v IN 10..=20 GROUP ALL;
SELECT count() FROM rv WITH NOINDEX WHERE v IN 10..=20 GROUP ALL;
