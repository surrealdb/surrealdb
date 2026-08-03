/**
[env]
namespace = true
database = true

planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
`rand::*` is evaluated once per row, never collapsed into a single constant
reused for the whole statement. The planner's literal folder evaluates
context-free calls at plan time, so it has to skip this family.

EXPLAIN reports the physical predicate, which states directly whether a call
survived planning or was replaced by its plan-time value. Asserting on the plan
keeps this exact: asserting on how many rows a random predicate matched would
only hold with high probability.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "{ attributes: { mode: 'GROUP ALL' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'no (unsupported predicate)', predicate: 'rand::float(...) < 0.5f', table: 't' }, context: 'Db', operator: 'TableScan' }], context: 'Db', expressions: [{ role: 'agg_arg', sql: 'NONE' }], operator: 'Aggregate' }"

[[test.results]]
value = "{ attributes: { mode: 'GROUP ALL' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'no (unsupported predicate)', predicate: 'rand::int(...) <= 50', table: 't' }, context: 'Db', operator: 'TableScan' }], context: 'Db', expressions: [{ role: 'agg_arg', sql: 'NONE' }], operator: 'Aggregate' }"

[[test.results]]
value = "{ attributes: { mode: 'GROUP ALL' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'no (unsupported predicate)', predicate: 'rand::bool(...)', table: 't' }, context: 'Db', operator: 'TableScan' }], context: 'Db', expressions: [{ role: 'agg_arg', sql: 'NONE' }], operator: 'Aggregate' }"

[[test.results]]
value = """{ attributes: { mode: 'GROUP ALL' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'no (unsupported predicate)', predicate: "rand::enum(...) = 'a'", table: 't' }, context: 'Db', operator: 'TableScan' }], context: 'Db', expressions: [{ role: 'agg_arg', sql: 'NONE' }], operator: 'Aggregate' }"""

[[test.results]]
value = "{ attributes: { mode: 'GROUP ALL' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'no (unsupported predicate)', predicate: 'rand::duration(...) < 50s', table: 't' }, context: 'Db', operator: 'TableScan' }], context: 'Db', expressions: [{ role: 'agg_arg', sql: 'NONE' }], operator: 'Aggregate' }"

[[test.results]]
value = """{ attributes: { mode: 'GROUP ALL' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'no (unsupported predicate)', predicate: "rand::string(...) != ''", table: 't' }, context: 'Db', operator: 'TableScan' }], context: 'Db', expressions: [{ role: 'agg_arg', sql: 'NONE' }], operator: 'Aggregate' }"""

[[test.results]]
value = """{ attributes: { mode: 'GROUP ALL' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'no (unsupported predicate)', predicate: "rand::uuid::v4(...) != u'00000000-0000-0000-0000-000000000000'", table: 't' }, context: 'Db', operator: 'TableScan' }], context: 'Db', expressions: [{ role: 'agg_arg', sql: 'NONE' }], operator: 'Aggregate' }"""

[[test.results]]
value = "{ attributes: { mode: 'GROUP ALL' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'no (unsupported predicate)', predicate: 'array::len(...) > 1', table: 't' }, context: 'Db', operator: 'TableScan' }], context: 'Db', expressions: [{ role: 'agg_arg', sql: 'NONE' }], operator: 'Aggregate' }"

[[test.results]]
value = "{ attributes: { mode: 'GROUP ALL' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'no (unsupported predicate)', predicate: '20f = 20', table: 't' }, context: 'Db', operator: 'TableScan' }], context: 'Db', expressions: [{ role: 'agg_arg', sql: 'NONE' }], operator: 'Aggregate' }"

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = "[{ count: 10 }]"
*/

CREATE |t:10| RETURN NONE;

-- Each predicate must still name the function. A folded call would appear here
-- as the literal it produced, the way `math::floor` does further down.
SELECT count() FROM t WHERE rand::float() < 0.5 GROUP ALL EXPLAIN;
SELECT count() FROM t WHERE rand::int(1, 100) <= 50 GROUP ALL EXPLAIN;
SELECT count() FROM t WHERE rand::bool() GROUP ALL EXPLAIN;
SELECT count() FROM t WHERE rand::enum('a', 'b') = 'a' GROUP ALL EXPLAIN;
SELECT count() FROM t WHERE rand::duration(0s, 100s) < 50s GROUP ALL EXPLAIN;
SELECT count() FROM t WHERE rand::string(8) != '' GROUP ALL EXPLAIN;
SELECT count() FROM t WHERE rand::uuid::v4() != u'00000000-0000-0000-0000-000000000000' GROUP ALL EXPLAIN;

-- EXPLAIN elides call arguments as `(...)`, so it can show that `array::map`
-- itself survived but says nothing about the closure body. The closure case is
-- asserted behaviourally further down instead.
SELECT count() FROM t WHERE array::len(array::distinct(array::map([1, 2], |$i| rand::float()))) > 1 GROUP ALL EXPLAIN;

-- A deterministic call must still fold, and fold to the right value.
SELECT count() FROM t WHERE math::floor(20.7) = 20 GROUP ALL EXPLAIN;

-- The unfolded calls must also evaluate at runtime. These predicates hold for
-- every draw, so the counts are exact rather than merely probable.
SELECT count() FROM t WHERE rand::float() >= 0.0 AND rand::float() <= 1.0 GROUP ALL;
SELECT count() FROM t WHERE rand::bool() IN [true, false] GROUP ALL;

-- The folding walk descends into closure bodies, so a call there needs the same
-- protection: two draws per row must not collapse to one repeated value. Folding
-- the body would make every row's pair identical and drop the count to zero.
-- Two v4 UUIDs colliding is a 2^-122 event, so 10 is exact for any real run.
SELECT count() FROM t WHERE array::len(array::distinct(array::map([1, 2], |$i| rand::uuid::v4()))) > 1 GROUP ALL;

-- Folding stays correct for the deterministic cases, `time::now()` included.
SELECT count() FROM t WHERE math::floor(20.7) = 20 GROUP ALL;
SELECT count() FROM t WHERE time::now() > d'2000-01-01T00:00:00Z' GROUP ALL;
