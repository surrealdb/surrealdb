/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "CONTAINS / INSIDE on the leading array-element column of a compound index must produce an IndexScan with a Compound prefix, so that index_covers_ordering can supply ORDER BY + LIMIT pushdown via a directional walk of the prefix-bounded range. Previously the analyser rejected composite indexes for containment predicates and fell back to TableScan."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 90, id: record:5 }, { age: 50, id: record:2 }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id, age] {rows: 2}\n    IndexScan [ctx: Db] [index: tag_age, access: ['omicron'], direction: Backward, limit: 2] {rows: 2}\n\nTotal rows: 2"'''

[[test.results]]
value = "[{ age: 10, id: record:6 }, { age: 20, id: record:1 }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id, age] {rows: 2}\n    IndexScan [ctx: Db] [index: tag_age, access: ['omicron'], direction: Forward, limit: 2] {rows: 2}\n\nTotal rows: 2"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 10, id: entry:3, score: 99 }, { age: 20, id: entry:2, score: 30 }, { age: 20, id: entry:1, score: 50 }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id, age, score] {rows: 3}\n    IndexScan [ctx: Db] [index: tag_age_score, access: ['omicron'], direction: Forward, limit: 3] {rows: 3}\n\nTotal rows: 3"'''

[[test.results]]
value = "[{ age: 30, id: entry:4, score: 70 }, { age: 20, id: entry:1, score: 50 }, { age: 20, id: entry:2, score: 30 }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id, age, score] {rows: 3}\n    IndexScan [ctx: Db] [index: tag_age_score, access: ['omicron'], direction: Backward, limit: 3] {rows: 3}\n\nTotal rows: 3"'''

[[test.results]]
value = "[{ age: 90, id: record:5 }, { age: 50, id: record:2 }, { age: 30, id: record:4 }]"

[[test.results]]
value = "[{ age: 10, id: record:6 }, { age: 20, id: record:1 }, { age: 30, id: record:4 }]"

[[test.results]]
value = "[{ age: 90, id: record:5 }, { age: 50, id: record:2 }, { age: 30, id: record:4 }, { age: 20, id: record:1 }, { age: 10, id: record:6 }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id, age] {rows: 5}\n    UnionIndexScan [ctx: Db] [table: record, branches: 2, merge_by_index_key_dedup: age Desc] {rows: 5}\n        IndexScan [ctx: Db] [index: tag_age, access: ['omicron'], direction: Backward] {rows: 5}\n        IndexScan [ctx: Db] [index: tag_age, access: ['beta'], direction: Backward] {rows: 1}\n\nTotal rows: 5"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id, age] {rows: 5}\n    UnionIndexScan [ctx: Db] [table: record, branches: 2, merge_by_index_key_dedup: age Asc] {rows: 5}\n        IndexScan [ctx: Db] [index: tag_age, access: ['omicron'], direction: Forward] {rows: 5}\n        IndexScan [ctx: Db] [index: tag_age, access: ['beta'], direction: Forward] {rows: 1}\n\nTotal rows: 5"'''

[[test.results]]
value = "[{ age: 50, id: record:2 }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id, age] {rows: 1}\n    Limit [ctx: Db] [limit: 3] {rows: 1}\n        Filter [ctx: Db] [predicate: tags CONTAINSALL ['omicron', 'beta']] {rows: 1}\n            UnionIndexScan [ctx: Db] [table: record, branches: 2, merge_by_index_key_dedup: age Desc] {rows: 5}\n                IndexScan [ctx: Db] [index: tag_age, access: ['omicron'], direction: Backward] {rows: 5}\n                IndexScan [ctx: Db] [index: tag_age, access: ['beta'], direction: Backward] {rows: 1}\n\nTotal rows: 1"'''

[[test.results]]
value = "[]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id, age] {rows: 0}\n    Limit [ctx: Db] [limit: 3] {rows: 0}\n        Filter [ctx: Db] [predicate: tags CONTAINSANY []] {rows: 0}\n            UnionIndexScan [ctx: Db] [table: record, branches: 1, merge_by_index_key_dedup: age Desc] {rows: 5}\n                IndexScan [ctx: Db] [index: tag_age, access: ['omicron'], direction: Backward] {rows: 5}\n\nTotal rows: 0"'''

*/

-- Setup: composite index whose leading column is an array-element flatten.
{
    DEFINE TABLE OVERWRITE record SCHEMALESS;
    DEFINE INDEX OVERWRITE tag_age ON record FIELDS tags.*, age;
    CREATE record:1 SET tags = ['alpha', 'omicron'], age = 20;
    CREATE record:2 SET tags = ['omicron', 'beta'], age = 50;
    CREATE record:3 SET tags = ['gamma'], age = 70;
    CREATE record:4 SET tags = ['omicron', 'delta'], age = 30;
    CREATE record:5 SET tags = ['omicron'], age = 90;
    CREATE record:6 SET tags = ['omicron', 'epsilon'], age = 10;
    RETURN NONE;
};

-- Case A: CONTAINS + ORDER BY trailing column DESC + LIMIT.
-- Expected top-2 omicron rows by age DESC: record:5 (90), record:2 (50).
SELECT id, age FROM record WHERE tags CONTAINS 'omicron' ORDER BY age DESC LIMIT 2;
EXPLAIN ANALYZE SELECT id, age FROM record WHERE tags CONTAINS 'omicron' ORDER BY age DESC LIMIT 2;

-- Case B: INSIDE on right-side idiom + ORDER BY ASC + LIMIT.
-- Expected bottom-2: record:6 (10), record:1 (20).
SELECT id, age FROM record WHERE 'omicron' INSIDE tags ORDER BY age ASC LIMIT 2;
EXPLAIN ANALYZE SELECT id, age FROM record WHERE 'omicron' INSIDE tags ORDER BY age ASC LIMIT 2;

-- Cases C / D: three-column composite index. Two trailing columns (`age`,
-- `score`) must both be covered by ORDER BY for the directional walk to
-- preserve ordering. This pins down `index_covers_ordering`'s
-- multi-trailing-column path for the containment-prefix shape.
{
    DEFINE TABLE OVERWRITE entry SCHEMALESS;
    DEFINE INDEX OVERWRITE tag_age_score ON entry FIELDS tags.*, age, score;
    CREATE entry:1 SET tags = ['omicron'], age = 20, score = 50;
    CREATE entry:2 SET tags = ['omicron'], age = 20, score = 30;
    CREATE entry:3 SET tags = ['omicron'], age = 10, score = 99;
    CREATE entry:4 SET tags = ['omicron'], age = 30, score = 70;
    CREATE entry:5 SET tags = ['other'],   age = 99, score = 99;
    RETURN NONE;
};

-- Case C: CONTAINS + ORDER BY two trailing columns ASC + LIMIT (Forward walk).
-- Expected order by (age ASC, score ASC): entry:3 (10, 99), entry:2 (20, 30),
-- entry:1 (20, 50).
SELECT id, age, score FROM entry
    WHERE tags CONTAINS 'omicron'
    ORDER BY age ASC, score ASC LIMIT 3;
EXPLAIN ANALYZE SELECT id, age, score FROM entry
    WHERE tags CONTAINS 'omicron'
    ORDER BY age ASC, score ASC LIMIT 3;

-- Case D: same index, both trailing columns DESC + LIMIT (Backward walk).
-- Expected order by (age DESC, score DESC): entry:4 (30, 70), entry:1 (20, 50),
-- entry:2 (20, 30).
SELECT id, age, score FROM entry
    WHERE tags CONTAINS 'omicron'
    ORDER BY age DESC, score DESC LIMIT 3;
EXPLAIN ANALYZE SELECT id, age, score FROM entry
    WHERE tags CONTAINS 'omicron'
    ORDER BY age DESC, score DESC LIMIT 3;

-- ============================================================================
-- Case E: CONTAINSANY with two values + ORDER BY trailing column + LIMIT.
-- Triggers the union path with ByIndexKeyDedup merge mode.
-- record:2 has BOTH 'omicron' and 'beta', so without dedupe it would be
-- emitted twice by the merge (once from each branch); LIMIT 3 would then
-- return record:5, record:2, record:2 instead of the correct top-3.
-- Expected top-3 by age DESC: record:5 (90), record:2 (50), record:4 (30).
-- The EXPLAIN ANALYZE proof of the plan shape lives in Case G below
-- on the no-LIMIT variant, because the per-operator `{rows: N}`
-- accounting on the union scan is timing-dependent when an outer
-- Limit cancels mid-stream (sub-scans buffer-fetch differently across
-- mem vs rocksdb vs surrealkv).
-- ============================================================================

SELECT id, age FROM record
    WHERE tags CONTAINSANY ['omicron', 'beta']
    ORDER BY age DESC LIMIT 3;

-- Case F: symmetric ANYINSIDE shape, ASC direction.
-- Expected bottom-3 by age ASC: record:6 (10), record:1 (20), record:4 (30).
SELECT id, age FROM record
    WHERE ['omicron', 'beta'] ANYINSIDE tags
    ORDER BY age ASC LIMIT 3;

-- Case G: regression check for dedupe correctness without LIMIT
-- cancellation, plus the EXPLAIN ANALYZE proof of the new plan shape.
-- All five rows containing 'omicron' or 'beta' must appear exactly once.
-- (record:1=20, record:2=50, record:4=30, record:5=90, record:6=10.)
-- record:2 sits in both branches' prefix ranges; the union's
-- `merge_by_index_key_dedup` HashSet drops the duplicate.
SELECT id, age FROM record
    WHERE tags CONTAINSANY ['omicron', 'beta']
    ORDER BY age DESC;
EXPLAIN ANALYZE SELECT id, age FROM record
    WHERE tags CONTAINSANY ['omicron', 'beta']
    ORDER BY age DESC;
EXPLAIN ANALYZE SELECT id, age FROM record
    WHERE ['omicron', 'beta'] ANYINSIDE tags
    ORDER BY age ASC;

-- ============================================================================
-- Case H: CONTAINSALL is documented-deferred. The analyser still emits a
-- Union of Compound branches, but `strip_union_index_conditions` does not
-- strip the predicate (intersection semantics), so the residual Filter
-- remains above the UnionIndexScan and LIMIT is not pushed into the scan.
-- This case pins that behaviour so the regression is visible if future
-- work adds an intersection operator (see issue #236).
-- record:2 is the only row with both 'omicron' and 'beta'.
-- ============================================================================

SELECT id, age FROM record
    WHERE tags CONTAINSALL ['omicron', 'beta']
    ORDER BY age DESC LIMIT 3;
EXPLAIN ANALYZE SELECT id, age FROM record
    WHERE tags CONTAINSALL ['omicron', 'beta']
    ORDER BY age DESC LIMIT 3;

-- ============================================================================
-- Case I: empty CONTAINSANY leaf must NOT be stripped.
-- `tags CONTAINSANY []` is FALSE by SurrealQL semantics (no values to match).
-- A naive `arr.iter().all(in_branch)` is vacuously true on the empty array
-- and would drop the always-false leaf — turning a 0-row query into a
-- non-empty one when another CONTAINSANY leaf provides the union path.
-- Combined here with `tags CONTAINSANY ['omicron']` so the union is built
-- and `strip_union_index_conditions` is exercised on the empty leaf.
-- Expected: zero rows; EXPLAIN keeps the residual `Filter` above the
-- UnionIndexScan and the outer `Limit`.
-- ============================================================================

SELECT id, age FROM record
    WHERE tags CONTAINSANY [] AND tags CONTAINSANY ['omicron']
    ORDER BY age DESC LIMIT 3;
EXPLAIN ANALYZE SELECT id, age FROM record
    WHERE tags CONTAINSANY [] AND tags CONTAINSANY ['omicron']
    ORDER BY age DESC LIMIT 3;
