/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "Same-side range bounds must tighten. `WHERE a > 5 AND a > 10` should produce a single IndexScan with `from > 10` rather than the looser `> 5` plus a residual filter. Similarly `WHERE a < 50 AND a < 20` should use `to < 20`."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 3, id: t:a }]"

[[test.results]]
value = "[{ a: 7, id: t:b }]"

[[test.results]]
value = "[{ a: 12, id: t:c }]"

[[test.results]]
value = "[{ a: 50, id: t:d }]"

[[test.results]]
value = "[{ a: 12, id: t:c }, { a: 50, id: t:d }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    Filter [ctx: Db] [predicate: a > 5]\n        IndexScan [ctx: Db] [index: idx_a, access: >10, direction: Forward]\n''''

[[test.results]]
value = "[{ a: 3, id: t:a }, { a: 7, id: t:b }, { a: 12, id: t:c }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    Filter [ctx: Db] [predicate: a < 50]\n        IndexScan [ctx: Db] [index: idx_a, access: <20, direction: Forward]\n''''

[[test.results]]
value = "[{ a: 7, id: t:b }, { a: 12, id: t:c }, { a: 50, id: t:d }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    Filter [ctx: Db] [predicate: a >= 5]\n        IndexScan [ctx: Db] [index: idx_a, access: >5, direction: Forward]\n''''
*/
DEFINE TABLE t;
DEFINE INDEX idx_a ON t FIELDS a;

CREATE t:a SET a = 3;
CREATE t:b SET a = 7;
CREATE t:c SET a = 12;
CREATE t:d SET a = 50;

-- Same-side `from` bounds: must pick the tighter (> 10).
SELECT * FROM t WHERE a > 5 AND a > 10 ORDER BY a;
EXPLAIN SELECT * FROM t WHERE a > 5 AND a > 10 ORDER BY a;

-- Same-side `to` bounds: must pick the tighter (< 20).
SELECT * FROM t WHERE a < 50 AND a < 20 ORDER BY a;
EXPLAIN SELECT * FROM t WHERE a < 50 AND a < 20 ORDER BY a;

-- Same value, mixed inclusive/exclusive: exclusive wins.
SELECT * FROM t WHERE a > 5 AND a >= 5 ORDER BY a;
EXPLAIN SELECT * FROM t WHERE a > 5 AND a >= 5 ORDER BY a;
