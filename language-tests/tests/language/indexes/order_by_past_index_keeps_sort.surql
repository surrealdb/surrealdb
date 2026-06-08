/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "When ORDER BY extends past the columns the index can satisfy, the planner MUST keep the Sort operator. Pre-fix, `analyze_order` only checked the first non-prefix ORDER BY field and overclaimed coverage for the rest, which would have eliminated the Sort and returned rows in the wrong order."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 20, c: 'z', id: t:r1 }]"

[[test.results]]
value = "[{ a: 1, b: 10, c: 'a', id: t:r2 }]"

[[test.results]]
value = "[{ a: 1, b: 10, c: 'm', id: t:r3 }]"

[[test.results]]
value = "[{ a: 1, b: 10, id: t:r2 }, { a: 1, b: 10, id: t:r3 }, { a: 1, b: 20, id: t:r1 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: a, b, id]\n    IndexScan [ctx: Db] [index: idx_ab, access: [1], direction: Forward]\n''''

[[test.results]]
value = "[{ a: 1, b: 10, c: 'a', id: t:r2 }, { a: 1, b: 10, c: 'm', id: t:r3 }, { a: 1, b: 20, c: 'z', id: t:r1 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: a, b, c, id]\n    SortByKey [ctx: Db] [sort_keys: a ASC, b ASC, c ASC]\n        IndexScan [ctx: Db] [index: idx_ab, access: [1], direction: Forward]\n''''
*/
DEFINE TABLE t;
DEFINE INDEX idx_ab ON t FIELDS a, b;

CREATE t:r1 SET a = 1, b = 20, c = 'z';
CREATE t:r2 SET a = 1, b = 10, c = 'a';
CREATE t:r3 SET a = 1, b = 10, c = 'm';

-- ORDER BY a, b is covered by INDEX(a, b): no Sort needed.
SELECT a, b, id FROM t WHERE a = 1 ORDER BY a, b;
EXPLAIN SELECT a, b, id FROM t WHERE a = 1 ORDER BY a, b;

-- ORDER BY a, b, c extends past the index. The compound scan delivers
-- (b, id) ordering only; `c` requires a real Sort.
SELECT a, b, c, id FROM t WHERE a = 1 ORDER BY a, b, c;
EXPLAIN SELECT a, b, c, id FROM t WHERE a = 1 ORDER BY a, b, c;
