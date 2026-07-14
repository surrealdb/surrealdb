/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Index-only COUNT via exact bitmap fusion: multi-index AND (and AND NOT) counted from index entries with zero record fetches"
issue = 547

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''"IndexCountScan [ctx: Db] [source: t, condition: a = 'x' AND b = true]\n    BitmapAnd [ctx: Db]\n        BitmapIndexScan [ctx: Db] [index: idx_a, access: = 'x']\n        BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]\n"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''"IndexCountScan [ctx: Db] [source: t, condition: a = 'x' AND not(...)]\n    BitmapAndNot [ctx: Db]\n        BitmapIndexScan [ctx: Db] [index: idx_a, access: = 'x']\n        BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]\n"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''"Aggregate [ctx: Db] [mode: GROUP ALL]\n    Filter [ctx: Db] [predicate: a = 'x' AND b = true AND id != t:1]\n        BitmapResolve [ctx: Db] [table: t]\n            BitmapAnd [ctx: Db]\n                BitmapIndexScan [ctx: Db] [index: idx_a, access: = 'x']\n                BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]\n"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 1 }]"
*/

DEFINE FIELD a ON t TYPE string;
DEFINE FIELD b ON t TYPE bool;
DEFINE INDEX idx_a ON t FIELDS a;
DEFINE INDEX idx_b ON t FIELDS b;

CREATE t:1 SET a = 'x', b = true RETURN NONE;
CREATE t:2 SET a = 'x', b = true RETURN NONE;
CREATE t:3 SET a = 'x', b = false RETURN NONE;
CREATE t:4 SET a = 'y', b = true RETURN NONE;

-- Fully index-backed AND: counted as the fused bitmap's cardinality.
SELECT count() FROM t WHERE a = 'x' AND b = true GROUP ALL;
EXPLAIN SELECT count() FROM t WHERE a = 'x' AND b = true GROUP ALL;

-- Conjunctive NOT: counted via bitmap subtraction.
SELECT count() FROM t WHERE a = 'x' AND NOT (b = true) GROUP ALL;
EXPLAIN SELECT count() FROM t WHERE a = 'x' AND NOT (b = true) GROUP ALL;

-- A conjunct not exactly index-covered (unindexed field): no bitmap count,
-- the operator falls back to scan + filter — results stay correct.
SELECT count() FROM t WHERE a = 'x' AND b = true AND id != t:1 GROUP ALL;
EXPLAIN SELECT count() FROM t WHERE a = 'x' AND b = true AND id != t:1 GROUP ALL;

-- NOINDEX baselines.
SELECT count() FROM t WITH NOINDEX WHERE a = 'x' AND b = true GROUP ALL;
SELECT count() FROM t WITH NOINDEX WHERE a = 'x' AND NOT (b = true) GROUP ALL;
