/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "`field IN []` must short-circuit to an empty result via AccessPath::EmptyScan. The literal folder rewrites the predicate to `false`, then the planner emits EmptyScan; the index is bypassed even though one exists on the field."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:a, name: 'alice' }]"

[[test.results]]
value = "[{ id: t:b, name: 'bob' }]"

[[test.results]]
value = "[{ id: t:a, name: 'alice' }, { id: t:b, name: 'bob' }]"

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
DEFINE INDEX idx_name ON t FIELDS name;

CREATE t:a SET name = 'alice';
CREATE t:b SET name = 'bob';
SELECT * FROM t ORDER BY name;

-- Empty IN: returns no rows.
SELECT * FROM t WHERE name IN [];
EXPLAIN SELECT * FROM t WHERE name IN [];

-- Empty IN combined with another predicate: still empty (short-circuit).
SELECT * FROM t WHERE name IN [] AND name = 'alice';
EXPLAIN SELECT * FROM t WHERE name IN [] AND name = 'alice';
