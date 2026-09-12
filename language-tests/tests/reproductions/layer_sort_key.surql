/**
[env]
namespace = true
database = true
auth = { level = "owner" }
# `compute-only` refuses `ORDER BY` on a field outside the selection, so the
# shape below only runs on the strategies that plan it.
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = "A sort over at most one row is only dropped when its keys are inert: a key naming a computed-field column is the only demand for that column, and `prune` removes a column nothing reads"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ bad: 'nope', id: t:one }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:one }]"

[[test.results]]
error = "Could not cast into `int` using input `'nope'`"

[[test.results]]
error = "Could not cast into `int` using input `'nope'`"

[[test.results]]
error = "Could not cast into `int` using input `'nope'`"
*/
DEFINE TABLE t SCHEMALESS;
CREATE t:one SET bad = 'nope';

-- The computed field is evaluated where it is read.
DEFINE FIELD c ON t COMPUTED <int> bad;

-- Nothing reads `c`, so nothing raises.
SELECT id FROM t:one;

-- The sort reads `c`, and it is the only thing that does: the cast raises
-- whether the source names a table, one record, or one record through
-- `ONLY`.
SELECT id FROM t ORDER BY c;
SELECT id FROM t:one ORDER BY c;
SELECT id FROM ONLY t:one ORDER BY c;
