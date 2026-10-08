/**
[test]
reason = "Events that take a record through every action of their own table in turn fire until their condition settles"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:start, n: 0 }]"

[[test.results]]
value = "[{ id: t:3, n: 3 }]"

[[test.results]]
value = "[{ id: t:3, n: 9 }]"

[[test.results]]
value = "[{ id: t:9, n: 9 }]"

[env]
namespace = "test"
database = "test"

*/

DEFINE TABLE t;
DEFINE EVENT on_create ON t WHEN $event = 'CREATE' AND $after.n < 3 THEN (UPDATE $after.id SET n += 1);
DEFINE EVENT on_update ON t WHEN $event = 'UPDATE' THEN (DELETE $after.id);
DEFINE EVENT on_delete ON t WHEN $event = 'DELETE' THEN (CREATE type::record('t', $before.n) SET n = $before.n);
CREATE t:start SET n = 0;
SELECT * FROM t ORDER BY id;
UPDATE t:3 SET n = 9;
SELECT * FROM t ORDER BY id;
