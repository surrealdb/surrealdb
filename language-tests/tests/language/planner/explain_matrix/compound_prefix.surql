/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = "Compound-index prefix construction, end to end: how far a prefix reaches for equality-only, equality-plus-range, gapped, reversed-order, contradictory and `!= NONE` conditions on two- and three-column indexes, and the containment extension by which a single-value CONTAINS / INSIDE pins a fanning column exactly as an equality pins a scalar one. Every indexed query is paired with its WITH NOINDEX oracle, and every fanning shape additionally with count() GROUP ALL, because a plan that looks right can still return the wrong rows. This file owns the containment extension and the inexact-prefix-over-a-fanning-column shape for the whole EXPLAIN matrix: array_fanout_oracle.surql pins what the fan-out KEY SET contains and defers prefix construction here. An index column whose idiom holds `*` stores one entry per element of that column's value, so a key range leaving such a column unpinned reaches a record through several entries and the scan is collapsed by a `Distinct` directly above it; where every fanning column IS pinned no `Distinct` appears, whether or not the prefix goes on to reach the index's trailing scalar columns. How far the prefix reaches and whether a `Distinct` follows are decided separately, and the file pins all three combinations: C1 and C2 are the pair on an index with ONE fanning column -- the most load-bearing contrast in the file -- and C10 and C11 restate it on an index with TWO, where pinning one of them extends the prefix across it and still leaves the `Distinct` standing for the other. Those four carry an EXPLAIN ANALYZE, where the scan's own `{rows: N}` counts index entries and the node above it counts records. Two cases -- E1 and E2 -- record WRONG answers, pinned rather than blessed: a scan over an unpinned fanning column carries no sort operator, so an explicit ORDER BY is dropped and the rows arrive in index-key order ascending, or, with the scan direction merely flipped, in reverse index-key order descending. Each says so where it stands and names the oracle as the correct answer; every other expectation in the file is correct. Narrowed to the streaming strategies because a prefix EXPLAIN is rejected under compute-only; both are pinned because they plan identically by contract, so recording the pair costs nothing and states the intent."

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: cp2_ab, access: [1], direction: Forward, predicate: a = 1]
''''

[[test.results]]
value = "[cp2:1, cp2:2, cp2:3]"

[[test.results]]
value = "[cp2:1, cp2:2, cp2:3]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: cp2_ab, access: [1, 20], direction: Forward, predicate: a = 1 AND b = 20]
''''

[[test.results]]
value = "[cp2:2]"

[[test.results]]
value = "[cp2:2]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: cp2_ab, access: [1] MoreThan 10, direction: Forward, predicate: a = 1 AND b > 10]
''''

[[test.results]]
value = "[cp2:2, cp2:3]"

[[test.results]]
value = "[cp2:2, cp2:3]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: cp2_ab, access: >1, direction: Forward, predicate: a > 1]
''''

[[test.results]]
value = "[cp2:4, cp2:5]"

[[test.results]]
value = "[cp2:4, cp2:5]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: cp2_ab, access: [1, 20], direction: Forward, predicate: b = 20 AND a = 1]
''''

[[test.results]]
value = "[cp2:2]"

[[test.results]]
value = "[cp2:2]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: cp2_ab, access: [1] MoreThan NONE, direction: Forward, predicate: a = 1 AND b != NONE]
''''

[[test.results]]
value = "[cp2:1, cp2:2, cp2:3]"

[[test.results]]
value = "[cp2:1, cp2:2, cp2:3]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: b != 20]
        IndexScan [ctx: Db] [index: cp2_ab, access: [1], direction: Forward]
''''

[[test.results]]
value = "[cp2:1, cp2:3]"

[[test.results]]
value = "[cp2:1, cp2:3]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: a = 2]
        IndexScan [ctx: Db] [index: cp2_ab, access: [1], direction: Forward]
''''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: cp3_abc, access: [1, 10, 'y'], direction: Forward, predicate: a = 1 AND b = 10 AND c = 'y']
"'''

[[test.results]]
value = "[cp3:2]"

[[test.results]]
value = "[cp3:2]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: cp3_abc, access: [1, 10] MoreThan 'x', direction: Forward, predicate: a = 1 AND b = 10 AND c > 'x']
"'''

[[test.results]]
value = "[cp3:2]"

[[test.results]]
value = "[cp3:2]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: c = 'x']
        IndexScan [ctx: Db] [index: cp3_abc, access: [1], direction: Forward]
"'''

[[test.results]]
value = "[cp3:1, cp3:3]"

[[test.results]]
value = "[cp3:1, cp3:3]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: c = 'x']
        IndexScan [ctx: Db] [index: cp3_abc, access: [1] MoreThan 10, direction: Forward]
"'''

[[test.results]]
value = "[cp3:3]"

[[test.results]]
value = "[cp3:3]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: cp3_abc, access: [1, 20, 'x'], direction: Forward, predicate: c = 'x' AND b = 20 AND a = 1]
"'''

[[test.results]]
value = "[cp3:3]"

[[test.results]]
value = "[cp3:3]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: cp3_abc, access: [1, 10], direction: Forward, predicate: a = 1 AND b = 10]
''''

[[test.results]]
value = "[cp3:1, cp3:2]"

[[test.results]]
value = "[cp3:1, cp3:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: fan_std_ae, access: ['acme', 'b@x'], direction: Forward, predicate: account = 'acme' AND emails.*.norm CONTAINS 'b@x']
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 2}
    IndexScan [ctx: Db] [index: fan_std_ae, access: ['acme', 'b@x'], direction: Forward, predicate: account = 'acme' AND emails.*.norm CONTAINS 'b@x'] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[fan_std:1, fan_std:2]"

[[test.results]]
value = "[fan_std:1, fan_std:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    DistinctRecords [ctx: Db]
        IndexScan [ctx: Db] [index: fan_std_ae, access: ['acme'], direction: Forward, predicate: account = 'acme']
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 2}
    DistinctRecords [ctx: Db] {rows: 2}
        IndexScan [ctx: Db] [index: fan_std_ae, access: ['acme'], direction: Forward, predicate: account = 'acme'] {rows: 4}

Total rows: 2"'''

[[test.results]]
value = "[fan_std:1, fan_std:2]"

[[test.results]]
value = "[fan_std:1, fan_std:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: fan_std_ae, access: ['acme', 'b@x'], direction: Forward, predicate: account = 'acme' AND 'b@x' INSIDE emails.*.norm]
"'''

[[test.results]]
value = "[fan_std:1, fan_std:2]"

[[test.results]]
value = "[fan_std:1, fan_std:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: emails.*.norm = 'b@x']
        DistinctRecords [ctx: Db]
            IndexScan [ctx: Db] [index: fan_std_ae, access: ['acme'], direction: Forward]
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: fan_std_ae, access: ['acme', 'zz@x'], direction: Forward, predicate: account = 'acme' AND emails.*.norm CONTAINS 'zz@x']
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: fan_uniq_ae, access: ['acme', 'b@x'], direction: Forward, predicate: account = 'acme' AND emails.*.norm CONTAINS 'b@x']
"'''

[[test.results]]
value = "[fan_uniq:1]"

[[test.results]]
value = "[fan_uniq:1]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: fan3_aet, access: ['acme', 'b@x', 2], direction: Forward, predicate: account = 'acme' AND emails.*.norm CONTAINS 'b@x' AND tier = 2]
"'''

[[test.results]]
value = "[fan3:2]"

[[test.results]]
value = "[fan3:2]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: fan3_aet, access: ['acme', 'b@x'], direction: Forward, predicate: account = 'acme' AND emails.*.norm CONTAINS 'b@x']
"'''

[[test.results]]
value = "[fan3:1, fan3:2]"

[[test.results]]
value = "[fan3:1, fan3:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: tier = 2]
        DistinctRecords [ctx: Db]
            IndexScan [ctx: Db] [index: fan3_aet, access: ['acme'], direction: Forward]
"'''

[[test.results]]
value = "[fan3:2]"

[[test.results]]
value = "[fan3:2]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    DistinctRecords [ctx: Db]
        IndexScan [ctx: Db] [index: fan2f_aet, access: ['acme', 'b@x'], direction: Forward, predicate: account = 'acme' AND emails.*.norm CONTAINS 'b@x']
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 2}
    DistinctRecords [ctx: Db] {rows: 2}
        IndexScan [ctx: Db] [index: fan2f_aet, access: ['acme', 'b@x'], direction: Forward, predicate: account = 'acme' AND emails.*.norm CONTAINS 'b@x'] {rows: 3}

Total rows: 2"'''

[[test.results]]
value = "[fan2f:1, fan2f:2]"

[[test.results]]
value = "[fan2f:1, fan2f:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: fan2f_aet, access: ['acme', 'b@x', 'q'], direction: Forward, predicate: account = 'acme' AND emails.*.norm CONTAINS 'b@x' AND tags.* CONTAINS 'q']
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 1}
    IndexScan [ctx: Db] [index: fan2f_aet, access: ['acme', 'b@x', 'q'], direction: Forward, predicate: account = 'acme' AND emails.*.norm CONTAINS 'b@x' AND tags.* CONTAINS 'q'] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[fan2f:1]"

[[test.results]]
value = "[fan2f:1]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    SortByKey [ctx: Db] [sort_keys: id ASC]
        IndexScan [ctx: Db] [index: cp2x_ab, access: [1], direction: Forward, predicate: a = 1]
''''

[[test.results]]
value = "[cp2x:1, cp2x:2, cp2x:3]"

[[test.results]]
value = "[cp2x:1, cp2x:2, cp2x:3]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: cp2x_ab, access: [1, 20], direction: Forward, predicate: a = 1 AND b = 20]
''''

[[test.results]]
value = "[cp2x:2]"

[[test.results]]
value = "[cp2x:2]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: fan_std_ae, access: ['acme', 'b@x'], direction: Forward, predicate: account = 'acme' AND emails.*.norm CONTAINS 'b@x']
"'''

[[test.results]]
value = "[fan_std:1, fan_std:2]"

[[test.results]]
value = "[fan_std:1, fan_std:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    DistinctRecords [ctx: Db]
        IndexScan [ctx: Db] [index: fan_ord_ae, access: ['acme'], direction: Forward, predicate: account = 'acme']
"'''

[[test.results]]
value = "[fan_ord:3, fan_ord:4, fan_ord:2, fan_ord:1]"

[[test.results]]
value = "[fan_ord:1, fan_ord:2, fan_ord:3, fan_ord:4]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    DistinctRecords [ctx: Db]
        IndexScan [ctx: Db] [index: fan_ord_ae, access: ['acme'], direction: Backward, predicate: account = 'acme']
"'''

[[test.results]]
value = "[fan_ord:1, fan_ord:4, fan_ord:2, fan_ord:3]"

[[test.results]]
value = "[fan_ord:4, fan_ord:3, fan_ord:2, fan_ord:1]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: fan_ord_ae, access: ['acme', 'b@x'], direction: Forward, predicate: account = 'acme' AND emails.*.norm CONTAINS 'b@x']
"'''

[[test.results]]
value = "[fan_ord:4]"

[[test.results]]
value = "[fan_ord:4]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: emails.*.norm CONTAINS 'c@x']
        IndexScan [ctx: Db] [index: fan_std_ae, access: ['acme', 'b@x'], direction: Forward]
"'''

[[test.results]]
value = "[fan_std:1]"

[[test.results]]
value = "[fan_std:1]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: fan_std, direction: Forward, predicate: emails.*.norm CONTAINS 'b@x', pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = "[fan_std:1, fan_std:2, fan_std:3]"

[[test.results]]
value = "[fan_std:1, fan_std:2, fan_std:3]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"
*/

-- ===========================================================================
-- Group A: two-column scalar index (a, b). Pins how far the prefix reaches
-- for each shape of leading condition, and which conditions survive as a
-- residual `predicate:` on the scan or as a separate `Filter`.
-- ===========================================================================

{
    DEFINE TABLE OVERWRITE cp2 SCHEMALESS;
    DEFINE INDEX OVERWRITE cp2_ab ON cp2 FIELDS a, b;
    CREATE cp2:1 SET a = 1, b = 10;
    CREATE cp2:2 SET a = 1, b = 20;
    CREATE cp2:3 SET a = 1, b = 30;
    CREATE cp2:4 SET a = 2, b = 10;
    CREATE cp2:5 SET a = 2, b = 20;
    RETURN NONE;
};

-- A1: equality on the leading column only. The prefix is one value wide and
-- the scan walks every key under it.
EXPLAIN SELECT VALUE id FROM cp2 WHERE a = 1;
SELECT VALUE id FROM cp2 WHERE a = 1 ORDER BY id;
SELECT VALUE id FROM cp2 WITH NOINDEX WHERE a = 1 ORDER BY id;

-- A2: equality on both columns. The prefix is exact, so the scan addresses a
-- single key.
EXPLAIN SELECT VALUE id FROM cp2 WHERE a = 1 AND b = 20;
SELECT VALUE id FROM cp2 WHERE a = 1 AND b = 20 ORDER BY id;
SELECT VALUE id FROM cp2 WITH NOINDEX WHERE a = 1 AND b = 20 ORDER BY id;

-- A3: equality on column 1 terminated by a range on column 2. The range is
-- encoded into the compound key bounds rather than left as a filter.
EXPLAIN SELECT VALUE id FROM cp2 WHERE a = 1 AND b > 10;
SELECT VALUE id FROM cp2 WHERE a = 1 AND b > 10 ORDER BY id;
SELECT VALUE id FROM cp2 WITH NOINDEX WHERE a = 1 AND b > 10 ORDER BY id;

-- A4: a range on the leading column. A range is a key bound, not a prefix
-- component, so `access:` carries the bound alone and no column is pinned. B4
-- is the shape where a condition on a later column exists behind such a range
-- and still cannot be seeked.
EXPLAIN SELECT VALUE id FROM cp2 WHERE a > 1;
SELECT VALUE id FROM cp2 WHERE a > 1 ORDER BY id;
SELECT VALUE id FROM cp2 WITH NOINDEX WHERE a > 1 ORDER BY id;

-- A5: the same conditions as A2 supplied in reverse textual order. Prefix
-- construction walks index columns and looks up a condition for each, so the
-- plan must be identical to A2 — order-independence, not luck.
EXPLAIN SELECT VALUE id FROM cp2 WHERE b = 20 AND a = 1;
SELECT VALUE id FROM cp2 WHERE b = 20 AND a = 1 ORDER BY id;
SELECT VALUE id FROM cp2 WITH NOINDEX WHERE b = 20 AND a = 1 ORDER BY id;

-- A6: `!= NONE` on the column after the exact prefix. A presence test is not
-- an equality, so it cannot pin the column and widen the exact prefix — but it
-- is a bound, so it is lowered into the key range exactly as A3's range is,
-- and leaves no residual `Filter`. A7 is the inequality that cannot be.
EXPLAIN SELECT VALUE id FROM cp2 WHERE a = 1 AND b != NONE;
SELECT VALUE id FROM cp2 WHERE a = 1 AND b != NONE ORDER BY id;
SELECT VALUE id FROM cp2 WITH NOINDEX WHERE a = 1 AND b != NONE ORDER BY id;

-- A7: an inequality on the column after the prefix. Distinguishes `!= NONE`
-- (a presence test) from `!= <value>` (an exclusion) in the same position.
EXPLAIN SELECT VALUE id FROM cp2 WHERE a = 1 AND b != 20;
SELECT VALUE id FROM cp2 WHERE a = 1 AND b != 20 ORDER BY id;
SELECT VALUE id FROM cp2 WITH NOINDEX WHERE a = 1 AND b != 20 ORDER BY id;

-- A8: a contradiction on the leading column. Two equalities on one column
-- cannot both hold, so the correct answer is zero rows whichever value the
-- prefix is built from.
EXPLAIN SELECT VALUE id FROM cp2 WHERE a = 1 AND a = 2;
SELECT VALUE id FROM cp2 WHERE a = 1 AND a = 2 ORDER BY id;
SELECT VALUE id FROM cp2 WITH NOINDEX WHERE a = 1 AND a = 2 ORDER BY id;

-- ===========================================================================
-- Group B: three-column scalar index (a, b, c). Adds the shapes that only a
-- third column can express: a full three-value prefix, a range terminating a
-- two-value prefix, and a GAP where the middle column is unconstrained.
-- ===========================================================================

{
    DEFINE TABLE OVERWRITE cp3 SCHEMALESS;
    DEFINE INDEX OVERWRITE cp3_abc ON cp3 FIELDS a, b, c;
    CREATE cp3:1 SET a = 1, b = 10, c = 'x';
    CREATE cp3:2 SET a = 1, b = 10, c = 'y';
    CREATE cp3:3 SET a = 1, b = 20, c = 'x';
    CREATE cp3:4 SET a = 1, b = 20, c = 'z';
    CREATE cp3:5 SET a = 2, b = 10, c = 'x';
    RETURN NONE;
};

-- B1: equality on all three columns — the widest exact prefix this index has.
EXPLAIN SELECT VALUE id FROM cp3 WHERE a = 1 AND b = 10 AND c = 'y';
SELECT VALUE id FROM cp3 WHERE a = 1 AND b = 10 AND c = 'y' ORDER BY id;
SELECT VALUE id FROM cp3 WITH NOINDEX WHERE a = 1 AND b = 10 AND c = 'y' ORDER BY id;

-- B2: two equalities terminated by a range on the third column.
EXPLAIN SELECT VALUE id FROM cp3 WHERE a = 1 AND b = 10 AND c > 'x';
SELECT VALUE id FROM cp3 WHERE a = 1 AND b = 10 AND c > 'x' ORDER BY id;
SELECT VALUE id FROM cp3 WITH NOINDEX WHERE a = 1 AND b = 10 AND c > 'x' ORDER BY id;

-- B3: GAP. Column 2 is unconstrained, so the prefix must stop at column 1 —
-- index keys are ordered by (a, b, c) and there is no contiguous range
-- holding all `c = 'x'` keys under `a = 1`. The `c` condition has to be
-- re-checked per fetched row.
EXPLAIN SELECT VALUE id FROM cp3 WHERE a = 1 AND c = 'x';
SELECT VALUE id FROM cp3 WHERE a = 1 AND c = 'x' ORDER BY id;
SELECT VALUE id FROM cp3 WITH NOINDEX WHERE a = 1 AND c = 'x' ORDER BY id;

-- B4: a range on column 2 with an equality on column 3 behind it. The range
-- terminates the prefix, so the `c` equality cannot be seeked even though a
-- condition for it exists.
EXPLAIN SELECT VALUE id FROM cp3 WHERE a = 1 AND b > 10 AND c = 'x';
SELECT VALUE id FROM cp3 WHERE a = 1 AND b > 10 AND c = 'x' ORDER BY id;
SELECT VALUE id FROM cp3 WITH NOINDEX WHERE a = 1 AND b > 10 AND c = 'x' ORDER BY id;

-- B5: all three equalities in fully reversed textual order.
EXPLAIN SELECT VALUE id FROM cp3 WHERE c = 'x' AND b = 20 AND a = 1;
SELECT VALUE id FROM cp3 WHERE c = 'x' AND b = 20 AND a = 1 ORDER BY id;
SELECT VALUE id FROM cp3 WITH NOINDEX WHERE c = 'x' AND b = 20 AND a = 1 ORDER BY id;

-- B6: a partial prefix on a wider index — two of three columns pinned.
EXPLAIN SELECT VALUE id FROM cp3 WHERE a = 1 AND b = 10;
SELECT VALUE id FROM cp3 WHERE a = 1 AND b = 10 ORDER BY id;
SELECT VALUE id FROM cp3 WITH NOINDEX WHERE a = 1 AND b = 10 ORDER BY id;

-- ===========================================================================
-- Group C: the containment extension, and the collapse an unpinned fanning
-- column requires. An index column whose idiom holds `*` stores one entry per
-- element of that column's value, so one record is reachable through several
-- entries of the same index. Two INDEPENDENT decisions follow from that, and
-- keeping them apart is what this group is for:
--
--   * How far the prefix reaches. A single-value `CONTAINS` / `INSIDE` on a
--     fanning column PINS it exactly as an equality pins a scalar one,
--     because an entry carrying the value at that position means exactly what
--     the containment asserts. The prefix then extends across consecutive
--     pinned columns, however many of them fan out.
--   * Whether the scan is collapsed. A column the prefix does NOT reach and
--     which fans out makes one record reachable through several keys in
--     range, so the scan is wrapped in `Distinct [ctx: Db]`. That node is
--     what makes each matching record reach the rest of the plan exactly
--     once, and it sits directly above the scan and below any residual
--     `Filter`: collapse the scan's output, then re-check. Under
--     `EXPLAIN ANALYZE` the scan's own `{rows: N}` counts index entries and
--     the `Distinct` above it counts records.
--
-- Three cases exhaust the combinations: every fanning column pinned (NO
-- `Distinct`, whether or not the prefix goes on to reach the index's trailing
-- scalar columns — C8 is the case where it does not), some fanning column
-- left unpinned (the prefix still reaches as far as it can and the `Distinct`
-- remains for the residue), and no fanning column at all (Groups A and B).
-- C1 and C2 are the first two on an index carrying a SINGLE fanning column,
-- where pinning it settles both decisions at once — the shortest statement of
-- the rule. C10 and C11 are the same two on an index carrying TWO, where the
-- decisions come apart: pinning one of them extends the prefix across it and
-- still leaves the `Distinct` standing for the other.
--
-- fan_std:1 carries three DISTINCT emails, so it occupies three entries under
-- its account and the two shapes differ in the plan, in the entry count and
-- in neither the rows nor `count()`.
-- ===========================================================================

{
    DEFINE TABLE OVERWRITE fan_std SCHEMALESS;
    DEFINE INDEX OVERWRITE fan_std_ae ON fan_std FIELDS account, emails.*.norm;
    CREATE fan_std:1 SET account = 'acme', emails = [{ norm: 'a@x' }, { norm: 'b@x' }, { norm: 'c@x' }];
    CREATE fan_std:2 SET account = 'acme', emails = [{ norm: 'b@x' }];
    CREATE fan_std:3 SET account = 'other', emails = [{ norm: 'a@x' }, { norm: 'b@x' }, { norm: 'zz@x' }];
    RETURN NONE;
};

LET $acct = 'acme';
LET $email = 'b@x';

-- C1: the headline shape — BOTH columns seeked. `access:` holds the account
-- and the email, the fanning column is pinned, and no `Distinct` stands above
-- the scan because there is nothing to collapse. Both leaves are proved by the
-- prefix, so no `Filter` stands above the scan; neither leaf is discharged
-- outright, though — the whole conjunction rides on the scan as its
-- `predicate:` attribute and is re-evaluated on each fetched row. That
-- re-check is recorded, not asserted as desirable — it runs over matching rows
-- only, so it is bounded by the answer rather than by the account. The
-- ANALYZE line pins the exactness directly: the scan reads exactly as many
-- entries as the answer has records.
EXPLAIN SELECT VALUE id FROM fan_std WHERE account = $acct AND emails.*.norm CONTAINS $email;
EXPLAIN ANALYZE SELECT VALUE id FROM fan_std WHERE account = $acct AND emails.*.norm CONTAINS $email;
SELECT VALUE id FROM fan_std WHERE account = $acct AND emails.*.norm CONTAINS $email ORDER BY id;
SELECT VALUE id FROM fan_std WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS $email ORDER BY id;
SELECT count() FROM fan_std WHERE account = $acct AND emails.*.norm CONTAINS $email GROUP ALL;
SELECT count() FROM fan_std WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS $email GROUP ALL;

-- C2: the same index and table seeked on the leading column ALONE — the
-- contrast C1 exists against. The fanning column is unpinned, so the
-- account's entries are walked one per element and `Distinct` stands directly
-- above the scan. Each matching record still reaches the statement exactly
-- once: the rows are the oracle's and `count()` is the record count. ANALYZE
-- separates the two tallies — the scan reports the entries it read, the
-- `Distinct` above it the records they resolve to.
EXPLAIN SELECT VALUE id FROM fan_std WHERE account = $acct;
EXPLAIN ANALYZE SELECT VALUE id FROM fan_std WHERE account = $acct;
SELECT VALUE id FROM fan_std WHERE account = $acct ORDER BY id;
SELECT VALUE id FROM fan_std WITH NOINDEX WHERE account = $acct ORDER BY id;
SELECT count() FROM fan_std WHERE account = $acct GROUP ALL;
SELECT count() FROM fan_std WITH NOINDEX WHERE account = $acct GROUP ALL;

-- C3: the reversed spelling of C1. `$e INSIDE field` is the same assertion
-- with the idiom on the right, and must produce the same prefix and the same
-- absence of a `Distinct`.
EXPLAIN SELECT VALUE id FROM fan_std WHERE account = $acct AND $email INSIDE emails.*.norm;
SELECT VALUE id FROM fan_std WHERE account = $acct AND $email INSIDE emails.*.norm ORDER BY id;
SELECT VALUE id FROM fan_std WITH NOINDEX WHERE account = $acct AND $email INSIDE emails.*.norm ORDER BY id;
SELECT count() FROM fan_std WHERE account = $acct AND $email INSIDE emails.*.norm GROUP ALL;
SELECT count() FROM fan_std WITH NOINDEX WHERE account = $acct AND $email INSIDE emails.*.norm GROUP ALL;

-- C4: an EQUALITY leaf on the fanning column, which is what the extension is
-- not. `emails.*.norm = $e` asserts that the whole flattened value equals the
-- string, not that one element does, so it cannot pin the column: `access:`
-- holds the account alone, the scan is collapsed by `Distinct`, and the leaf
-- survives as a re-check that no record passes. Containment is the only leaf
-- that pins a fanning column, which is the premise C1 rests on.
EXPLAIN SELECT VALUE id FROM fan_std WHERE account = $acct AND emails.*.norm = $email;
SELECT VALUE id FROM fan_std WHERE account = $acct AND emails.*.norm = $email ORDER BY id;
SELECT VALUE id FROM fan_std WITH NOINDEX WHERE account = $acct AND emails.*.norm = $email ORDER BY id;
SELECT count() FROM fan_std WHERE account = $acct AND emails.*.norm = $email GROUP ALL;
SELECT count() FROM fan_std WITH NOINDEX WHERE account = $acct AND emails.*.norm = $email GROUP ALL;

-- C5: a value carried by a row of the OTHER account and by no row of the
-- requested one. The containment alone would reach fan_std:3, so an empty
-- answer is only available to a seek that pins the leading column too — which
-- is what makes this case discriminate rather than merely come back empty.
EXPLAIN SELECT VALUE id FROM fan_std WHERE account = $acct AND emails.*.norm CONTAINS 'zz@x';
SELECT VALUE id FROM fan_std WHERE account = $acct AND emails.*.norm CONTAINS 'zz@x' ORDER BY id;
SELECT VALUE id FROM fan_std WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS 'zz@x' ORDER BY id;
SELECT count() FROM fan_std WHERE account = $acct AND emails.*.norm CONTAINS 'zz@x' GROUP ALL;
SELECT count() FROM fan_std WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS 'zz@x' GROUP ALL;

-- C6: UNIQUE carrying the same shape. Uniqueness is per (account, element)
-- pair, so a record with three emails occupies three entries and the
-- extension has to behave identically.
{
    DEFINE TABLE OVERWRITE fan_uniq SCHEMALESS;
    DEFINE INDEX OVERWRITE fan_uniq_ae ON fan_uniq FIELDS account, emails.*.norm UNIQUE;
    CREATE fan_uniq:1 SET account = 'acme', emails = [{ norm: 'a@x' }, { norm: 'b@x' }, { norm: 'c@x' }];
    CREATE fan_uniq:2 SET account = 'acme', emails = [{ norm: 'd@x' }];
    CREATE fan_uniq:3 SET account = 'other', emails = [{ norm: 'a@x' }, { norm: 'b@x' }];
    RETURN NONE;
};

EXPLAIN SELECT VALUE id FROM fan_uniq WHERE account = $acct AND emails.*.norm CONTAINS $email;
SELECT VALUE id FROM fan_uniq WHERE account = $acct AND emails.*.norm CONTAINS $email ORDER BY id;
SELECT VALUE id FROM fan_uniq WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS $email ORDER BY id;
SELECT count() FROM fan_uniq WHERE account = $acct AND emails.*.norm CONTAINS $email GROUP ALL;
SELECT count() FROM fan_uniq WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS $email GROUP ALL;

-- C7: a three-column index whose MIDDLE column fans out. The containment pins
-- column 2, and a scalar equality on column 3 can then extend the prefix
-- behind it — the extension does not terminate prefix construction. All three
-- columns are pinned, and the fanning one among them is why the scan carries
-- no `Distinct`.
{
    DEFINE TABLE OVERWRITE fan3 SCHEMALESS;
    DEFINE INDEX OVERWRITE fan3_aet ON fan3 FIELDS account, emails.*.norm, tier;
    CREATE fan3:1 SET account = 'acme', emails = [{ norm: 'a@x' }, { norm: 'b@x' }, { norm: 'c@x' }], tier = 1;
    CREATE fan3:2 SET account = 'acme', emails = [{ norm: 'b@x' }], tier = 2;
    CREATE fan3:3 SET account = 'other', emails = [{ norm: 'a@x' }], tier = 1;
    RETURN NONE;
};

EXPLAIN SELECT VALUE id FROM fan3 WHERE account = $acct AND emails.*.norm CONTAINS $email AND tier = 2;
SELECT VALUE id FROM fan3 WHERE account = $acct AND emails.*.norm CONTAINS $email AND tier = 2 ORDER BY id;
SELECT VALUE id FROM fan3 WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS $email AND tier = 2 ORDER BY id;
SELECT count() FROM fan3 WHERE account = $acct AND emails.*.norm CONTAINS $email AND tier = 2 GROUP ALL;
SELECT count() FROM fan3 WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS $email AND tier = 2 GROUP ALL;

-- C8: the same three-column index with the trailing column unconstrained — a
-- two-value prefix on a three-column index. The fanning column is still
-- pinned, so the trailing scalar varying under it costs no `Distinct`: one
-- entry per (account, element, tier) triple, and the element is fixed.
EXPLAIN SELECT VALUE id FROM fan3 WHERE account = $acct AND emails.*.norm CONTAINS $email;
SELECT VALUE id FROM fan3 WHERE account = $acct AND emails.*.norm CONTAINS $email ORDER BY id;
SELECT VALUE id FROM fan3 WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS $email ORDER BY id;
SELECT count() FROM fan3 WHERE account = $acct AND emails.*.norm CONTAINS $email GROUP ALL;
SELECT count() FROM fan3 WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS $email GROUP ALL;

-- C9: the same three-column index reached through a GAP — containment on
-- column 2 is absent, so `tier` cannot be seeked either and the fanning
-- column is left unpinned. This is the node-order case: `Distinct` sits
-- between the residual `Filter` and the scan, because the collapse belongs on
-- the scan's output and the re-check above it.
EXPLAIN SELECT VALUE id FROM fan3 WHERE account = $acct AND tier = 2;
SELECT VALUE id FROM fan3 WHERE account = $acct AND tier = 2 ORDER BY id;
SELECT VALUE id FROM fan3 WITH NOINDEX WHERE account = $acct AND tier = 2 ORDER BY id;
SELECT count() FROM fan3 WHERE account = $acct AND tier = 2 GROUP ALL;
SELECT count() FROM fan3 WITH NOINDEX WHERE account = $acct AND tier = 2 GROUP ALL;

-- C10 and C11: TWO fanning columns on one index, which is the shape that can
-- state the prefix and the collapse as separate decisions. fan2f:1 carries
-- three emails and two tags, so it occupies six entries under 'acme'.
{
    DEFINE TABLE OVERWRITE fan2f SCHEMALESS;
    DEFINE INDEX OVERWRITE fan2f_aet ON fan2f FIELDS account, emails.*.norm, tags.*;
    CREATE fan2f:1 SET account = 'acme', emails = [{ norm: 'a@x' }, { norm: 'b@x' }, { norm: 'c@x' }], tags = ['p', 'q'];
    CREATE fan2f:2 SET account = 'acme', emails = [{ norm: 'b@x' }], tags = ['p'];
    CREATE fan2f:3 SET account = 'other', emails = [{ norm: 'a@x' }], tags = ['q'];
    RETURN NONE;
};

-- C10: one of the two fanning columns pinned. The containment on
-- `emails.*.norm` pins column 2, so the prefix reaches it — `access:` holds
-- the account AND the email — while `tags.*` is left unpinned and still
-- multiplies the keys under that prefix. The prefix therefore reaches as far
-- as it can AND the `Distinct` remains, for the tag residue alone: the two
-- are decided separately. ANALYZE separates the tallies — the scan reports
-- the entries under ('acme', 'b@x'), the `Distinct` above it the records they
-- resolve to. The rows are the oracle's and `count()` is the record count.
EXPLAIN SELECT VALUE id FROM fan2f WHERE account = $acct AND emails.*.norm CONTAINS $email;
EXPLAIN ANALYZE SELECT VALUE id FROM fan2f WHERE account = $acct AND emails.*.norm CONTAINS $email;
SELECT VALUE id FROM fan2f WHERE account = $acct AND emails.*.norm CONTAINS $email ORDER BY id;
SELECT VALUE id FROM fan2f WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS $email ORDER BY id;
SELECT count() FROM fan2f WHERE account = $acct AND emails.*.norm CONTAINS $email GROUP ALL;
SELECT count() FROM fan2f WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS $email GROUP ALL;

-- C11: the same index with BOTH fanning columns pinned — a containment leaf
-- on each. The prefix reaches all three columns, and because no column it
-- fails to reach fans out there is NOTHING to collapse: the scan carries NO
-- `Distinct`. That absence against C10's presence, on one index and one
-- table, is the load-bearing half of the contrast; the tag narrows the answer
-- to the single record carrying both values, and ANALYZE pins the exactness
-- directly — the scan reads exactly as many entries as the answer has
-- records. Both containment leaves survive as a `Filter` above the scan for
-- the reason C1 records, while the account equality, pinned by the same
-- prefix, is stripped.
EXPLAIN SELECT VALUE id FROM fan2f WHERE account = $acct AND emails.*.norm CONTAINS $email AND tags.* CONTAINS 'q';
EXPLAIN ANALYZE SELECT VALUE id FROM fan2f WHERE account = $acct AND emails.*.norm CONTAINS $email AND tags.* CONTAINS 'q';
SELECT VALUE id FROM fan2f WHERE account = $acct AND emails.*.norm CONTAINS $email AND tags.* CONTAINS 'q' ORDER BY id;
SELECT VALUE id FROM fan2f WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS $email AND tags.* CONTAINS 'q' ORDER BY id;
SELECT count() FROM fan2f WHERE account = $acct AND emails.*.norm CONTAINS $email AND tags.* CONTAINS 'q' GROUP ALL;
SELECT count() FROM fan2f WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS $email AND tags.* CONTAINS 'q' GROUP ALL;

-- ===========================================================================
-- Group D: what a prefix's exactness is worth to ORDER BY. A prefix reaching
-- every indexed column addresses one key per record, so index order IS
-- record-id order; an INEXACT prefix leaves the scan walking keys ordered by
-- the trailing index columns, which is not. `ORDER BY id` on such a scan must
-- still be honoured.
--
-- cp2x's ids run opposite to its `b` values, so index order and id order
-- disagree and an eliminated sort is visible in the rows themselves.
-- ===========================================================================

{
    DEFINE TABLE OVERWRITE cp2x SCHEMALESS;
    DEFINE INDEX OVERWRITE cp2x_ab ON cp2x FIELDS a, b;
    CREATE cp2x:1 SET a = 1, b = 30;
    CREATE cp2x:2 SET a = 1, b = 20;
    CREATE cp2x:3 SET a = 1, b = 10;
    RETURN NONE;
};

-- D1: prefix on the leading column only, so several keys share it and their
-- order is `b`'s, not `id`'s. A `SortByKey` is inserted and the rows come
-- back in id order.
EXPLAIN SELECT VALUE id FROM cp2x WHERE a = 1 ORDER BY id;
SELECT VALUE id FROM cp2x WHERE a = 1 ORDER BY id;
SELECT VALUE id FROM cp2x WITH NOINDEX WHERE a = 1 ORDER BY id;

-- D2: an exact prefix addresses one key, so ordering is trivially satisfied.
-- The contrast with D1 is the point.
EXPLAIN SELECT VALUE id FROM cp2x WHERE a = 1 AND b = 20 ORDER BY id;
SELECT VALUE id FROM cp2x WHERE a = 1 AND b = 20 ORDER BY id;
SELECT VALUE id FROM cp2x WITH NOINDEX WHERE a = 1 AND b = 20 ORDER BY id;

-- D3: the fanning equivalent of D2 on the containment index — an EXACT prefix
-- over an index carrying a `*` column. No sort operator is inserted and none
-- is needed: pinning every indexed column leaves the record id as the only
-- varying part of the key, so index order IS id order. E1 is the same absent
-- sort operator where the fanning column is unpinned, and there it is wrong.
EXPLAIN SELECT VALUE id FROM fan_std WHERE account = $acct AND emails.*.norm CONTAINS $email ORDER BY id;
SELECT VALUE id FROM fan_std WHERE account = $acct AND emails.*.norm CONTAINS $email ORDER BY id;
SELECT VALUE id FROM fan_std WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS $email ORDER BY id;
SELECT count() FROM fan_std WHERE account = $acct AND emails.*.norm CONTAINS $email GROUP ALL;
SELECT count() FROM fan_std WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS $email GROUP ALL;

-- ===========================================================================
-- Group E: what an inexact prefix over a fanning column costs an explicit
-- ORDER BY, and the two containment leaves that cannot both extend one
-- column.
--
-- fan_ord's ids run opposite to its `norm` values under one account, so index
-- order and id order disagree in both directions; fan_ord:4 carries two
-- distinct elements, so its entries interleave with the other records'.
-- ===========================================================================

{
    DEFINE TABLE OVERWRITE fan_ord SCHEMALESS;
    DEFINE INDEX OVERWRITE fan_ord_ae ON fan_ord FIELDS account, emails.*.norm;
    CREATE fan_ord:1 SET account = 'acme', emails = [{ norm: 'z@x' }];
    CREATE fan_ord:2 SET account = 'acme', emails = [{ norm: 'm@x' }];
    CREATE fan_ord:3 SET account = 'acme', emails = [{ norm: 'a@x' }];
    CREATE fan_ord:4 SET account = 'acme', emails = [{ norm: 'b@x' }, { norm: 'y@x' }];
    RETURN NONE;
};

-- E1: an inexact prefix on a fanning index with an explicit `ORDER BY id`.
--
-- DEFECT PINNED, in the ordering alone. The `Distinct` above the scan holds
-- record uniqueness — each of the four records appears once and `count()` is
-- 4, matching the oracle — but the plan carries no sort operator, so the
-- ORDER BY is dropped and the rows arrive in index-key order:
-- [fan_ord:3, fan_ord:4, fan_ord:2, fan_ord:1] where the oracle's
-- [fan_ord:1, fan_ord:2, fan_ord:3, fan_ord:4] is the correct answer.
--
-- The loss is attributable, and two other cases in this file are what
-- attribute it. D1 is the identical inexact-prefix shape on a scalar compound
-- index and DOES receive a `SortByKey`, so the loss needs a `*` column to
-- occur. D3 reaches a fanning index through an exact prefix and is correctly
-- ordered with no sort operator, so it is prefix inexactness and not the
-- fanning index itself that costs the ordering.
EXPLAIN SELECT VALUE id FROM fan_ord WHERE account = $acct ORDER BY id;
SELECT VALUE id FROM fan_ord WHERE account = $acct ORDER BY id;
SELECT VALUE id FROM fan_ord WITH NOINDEX WHERE account = $acct ORDER BY id;
SELECT count() FROM fan_ord WHERE account = $acct GROUP ALL;
SELECT count() FROM fan_ord WITH NOINDEX WHERE account = $acct GROUP ALL;

-- E2: the same shape ordered DESCENDING, where index-key order cannot
-- coincide with the requested order even by accident.
--
-- DEFECT PINNED, the same one as E1. The scan direction flips to `Backward`
-- in place of a sort, so the rows come back in reverse index-key order —
-- [fan_ord:1, fan_ord:4, fan_ord:2, fan_ord:3] against the oracle's correct
-- [fan_ord:4, fan_ord:3, fan_ord:2, fan_ord:1]. A flipped scan direction
-- serves an ORDER BY only when the varying tail of the key IS the sort key,
-- which an unpinned fanning column is what denies.
EXPLAIN SELECT VALUE id FROM fan_ord WHERE account = $acct ORDER BY id DESC;
SELECT VALUE id FROM fan_ord WHERE account = $acct ORDER BY id DESC;
SELECT VALUE id FROM fan_ord WITH NOINDEX WHERE account = $acct ORDER BY id DESC;
SELECT count() FROM fan_ord WHERE account = $acct GROUP ALL;
SELECT count() FROM fan_ord WITH NOINDEX WHERE account = $acct GROUP ALL;

-- E3: the same table and account reached through an exact prefix, which
-- narrows the answer to the one record carrying `b@x`. Every record occupies a
-- single entry under `('acme', 'b@x')`, so the scan needs no `Distinct` and
-- the ordering is satisfied by the key order itself.
EXPLAIN SELECT VALUE id FROM fan_ord WHERE account = $acct AND emails.*.norm CONTAINS $email ORDER BY id;
SELECT VALUE id FROM fan_ord WHERE account = $acct AND emails.*.norm CONTAINS $email ORDER BY id;
SELECT VALUE id FROM fan_ord WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS $email ORDER BY id;
SELECT count() FROM fan_ord WHERE account = $acct AND emails.*.norm CONTAINS $email GROUP ALL;
SELECT count() FROM fan_ord WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS $email GROUP ALL;

-- E4: two containment leaves on ONE fanning column. A single index entry
-- holds a single element, so only one leaf can be seeked. That leaf pins the
-- column, so the scan carries no `Distinct`, and BOTH leaves are re-checked
-- above it — the seeked one for the reason C1 records, the other because no
-- prefix proves it. Only fan_std:1 carries both values.
EXPLAIN SELECT VALUE id FROM fan_std WHERE account = $acct AND emails.*.norm CONTAINS 'b@x' AND emails.*.norm CONTAINS 'c@x';
SELECT VALUE id FROM fan_std WHERE account = $acct AND emails.*.norm CONTAINS 'b@x' AND emails.*.norm CONTAINS 'c@x' ORDER BY id;
SELECT VALUE id FROM fan_std WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS 'b@x' AND emails.*.norm CONTAINS 'c@x' ORDER BY id;
SELECT count() FROM fan_std WHERE account = $acct AND emails.*.norm CONTAINS 'b@x' AND emails.*.norm CONTAINS 'c@x' GROUP ALL;
SELECT count() FROM fan_std WITH NOINDEX WHERE account = $acct AND emails.*.norm CONTAINS 'b@x' AND emails.*.norm CONTAINS 'c@x' GROUP ALL;

-- E5: containment on column 2 with column 1 unconstrained — a GAP at the head
-- of the index. The extension cannot apply to a column that no prefix
-- reaches, so nothing is seeked and the plan falls to a table scan, which
-- needs no collapse because it visits each record once by construction.
EXPLAIN SELECT VALUE id FROM fan_std WHERE emails.*.norm CONTAINS $email;
SELECT VALUE id FROM fan_std WHERE emails.*.norm CONTAINS $email ORDER BY id;
SELECT VALUE id FROM fan_std WITH NOINDEX WHERE emails.*.norm CONTAINS $email ORDER BY id;
SELECT count() FROM fan_std WHERE emails.*.norm CONTAINS $email GROUP ALL;
SELECT count() FROM fan_std WITH NOINDEX WHERE emails.*.norm CONTAINS $email GROUP ALL;
