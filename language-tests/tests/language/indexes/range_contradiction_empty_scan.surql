/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "Contradictory range predicates (a > 10 AND a < 5) must produce zero rows and EmptyScan should appear in the EXPLAIN plan — the analyzer detects the contradiction during range merging and emits AccessPath::EmptyScan instead of issuing an index scan that finds nothing. Same for `a >= 5 AND a < 5`."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 7, id: t:a }]"

[[test.results]]
value = "[{ a: 12, id: t:b }]"

[[test.results]]
value = "[{ a: 3, id: t:c }]"

[[test.results]]
value = "[{ a: 3, id: t:c }, { a: 7, id: t:a }, { a: 12, id: t:b }]"

[[test.results]]
value = "[]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    EmptyScan [ctx: Rt]\n''''

[[test.results]]
value = "[]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    EmptyScan [ctx: Rt]\n''''
*/
DEFINE TABLE t;
DEFINE INDEX idx_a ON t FIELDS a;

CREATE t:a SET a = 7;
CREATE t:b SET a = 12;
CREATE t:c SET a = 3;
SELECT * FROM t ORDER BY a;

-- Contradiction within a single column: no row can satisfy both bounds.
SELECT * FROM t WHERE a > 10 AND a < 5;
EXPLAIN SELECT * FROM t WHERE a > 10 AND a < 5;

-- Equal values with one exclusive bound: still empty.
SELECT * FROM t WHERE a >= 5 AND a < 5;
EXPLAIN SELECT * FROM t WHERE a >= 5 AND a < 5;
