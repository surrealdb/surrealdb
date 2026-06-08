/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "When the analyzer turns `a IN [v]` into `a = v` (single-element IN normalised in `extract_simple_condition`) or encodes `b != NONE` after an equality prefix as `(MoreThan, NONE)`, the residual stripper in `IndexConditionMatcher` must mirror both rewrites. Without the fix the original AST leaves survived in the residual Filter even though the IndexScan already satisfied them, costing per-row predicate evaluation."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 10, id: t:r1 }]"

[[test.results]]
value = "[{ a: 1, b: 20, id: t:r2 }]"

[[test.results]]
value = "[{ a: 2, b: 10, id: t:r3 }]"

[[test.results]]
value = "[{ id: t:r1 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id]\n    IndexScan [ctx: Db] [index: idx_ab, access: [1, 10], direction: Forward]\n''''

[[test.results]]
value = "[{ id: t:r1 }, { id: t:r2 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id]\n    IndexScan [ctx: Db] [index: idx_ab, access: [1] MoreThan NONE, direction: Forward]\n''''
*/
DEFINE TABLE t;
DEFINE INDEX idx_ab ON t FIELDS a, b;

CREATE t:r1 SET a = 1, b = 10;
CREATE t:r2 SET a = 1, b = 20;
CREATE t:r3 SET a = 2, b = 10;

-- `a IN [1] AND b = 10` must use the index for BOTH columns and
-- leave NO residual Filter (the single-element IN normalises to `=`).
SELECT id FROM t WHERE a IN [1] AND b = 10;
EXPLAIN SELECT id FROM t WHERE a IN [1] AND b = 10;

-- `a = 1 AND b != NONE` after the equality prefix encodes the
-- non-equality as `(MoreThan, NONE)` in the compound access. The
-- residual stripper must accept the original `!= NONE` leaf so no
-- Filter wraps the IndexScan.
SELECT id FROM t WHERE a = 1 AND b != NONE;
EXPLAIN SELECT id FROM t WHERE a = 1 AND b != NONE;
