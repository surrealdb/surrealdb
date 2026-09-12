/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A join keyed on the right table's id is read by a batched get of the left keys only where the chain over the table is inert: the scalar form the lift replaced reads the whole relation, so a predicate that raises on a row no left key names is an error the plan owes"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: team:a, ok: true }]"

[[test.results]]
value = "[{ bad: 'nope', id: team:b }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, k: team:a }]"

[[test.results]]
value = "[t:1]"

[[test.results]]
error = "Could not cast into `int` using input `'nope'`"
*/
DEFINE TABLE team SCHEMALESS;
CREATE team:a SET ok = true;
CREATE team:b SET bad = 'nope';
DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET k = team:a;

-- Nothing in the chain over `team` can raise, so the right side may be read
-- by the left key alone.
SELECT VALUE id FROM t WHERE k IN (SELECT VALUE id FROM team WHERE ok = true);

-- The predicate raises on `team:b`, which no left key names. Reading the
-- whole relation is what the statement asks for.
SELECT VALUE id FROM t WHERE k IN (SELECT VALUE id FROM team WHERE IF id = team:b THEN <int> bad ELSE true END);
