/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "An OR branch that the analyzer can prove is empty (e.g. a contradictory range) must be dropped from the union — a zero-row branch contributes nothing to OR semantics. Without this fix `try_or_union` pushed an `AccessPath::EmptyScan` sub-path into the Union and `build_union_sub_operator` errored with `Error::Internal(\"UnionIndexScan sub-path produced an unexpected access path\")`. When only one branch survives the result is a plain IndexScan, not a Union."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 7, b: 1, id: t:r1 }]"

[[test.results]]
value = "[{ a: 7, b: 2, id: t:r2 }]"

[[test.results]]
value = "[{ a: 7, b: 3, id: t:r3 }]"

[[test.results]]
value = "[{ id: t:r1 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id]\n    Filter [ctx: Db] [predicate: a > 10 AND a < 5 OR b = 1]\n        IndexScan [ctx: Db] [index: idx_b, access: = 1, direction: Forward]\n''''

[[test.results]]
value = "[]"

[[test.results]]
value = ''''SelectProject [ctx: Rt] [projections: id]\n    EmptyScan [ctx: Rt]\n''''
*/
DEFINE TABLE t;
DEFINE INDEX idx_a ON t FIELDS a;
DEFINE INDEX idx_b ON t FIELDS b;

CREATE t:r1 SET a = 7, b = 1;
CREATE t:r2 SET a = 7, b = 2;
CREATE t:r3 SET a = 7, b = 3;

-- The `a > 10 AND a < 5` branch is empty after range merging, so the
-- OR collapses to `b = 1` and the planner emits a single IndexScan
-- on idx_b (no Union, no Filter for the dead branch).
SELECT id FROM t WHERE (a > 10 AND a < 5) OR b = 1;
EXPLAIN SELECT id FROM t WHERE (a > 10 AND a < 5) OR b = 1;

-- Every branch is empty, so the whole OR is empty.
SELECT id FROM t WHERE (a > 10 AND a < 5) OR (a > 100 AND a < 50);
EXPLAIN SELECT id FROM t WHERE (a > 10 AND a < 5) OR (a > 100 AND a < 50);
