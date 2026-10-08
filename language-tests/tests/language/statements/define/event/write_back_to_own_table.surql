/**
[test]
reason = "Events whose clauses write back to their own table, directly and through another table's events, fire until their conditions settle"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: counter:1, n: 0, seen: [] }]"

[[test.results]]
value = "[{ id: counter:1, n: 5, seen: [0, 1, 2, 3, 4] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: ping:start, n: 0 }]"

[[test.results]]
value = "[{ id: ping:1, n: 2 }, { id: ping:3, n: 4 }, { id: ping:start, n: 0 }]"

[[test.results]]
value = "[{ id: pong:0, n: 1 }, { id: pong:2, n: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ depth: 0, id: log:root }]"

[[test.results]]
value = "[{ depth: 0 }, { depth: 1 }, { depth: 2 }, { depth: 3 }]"

[env]
namespace = "test"
database = "test"

*/

DEFINE TABLE counter;
DEFINE EVENT bump ON counter WHEN $after.n < 5 THEN (UPDATE $after.id SET n += 1, seen += [$after.n]);
CREATE counter:1 SET n = 0, seen = [];
SELECT * FROM counter;

DEFINE EVENT to_pong ON ping WHEN $after.n < 4 THEN (UPSERT type::record('pong', $after.n) SET n = $after.n + 1);
DEFINE EVENT to_ping ON pong WHEN $after.n < 4 THEN (UPSERT type::record('ping', $after.n) SET n = $after.n + 1);
UPSERT ping:start SET n = 0;
SELECT * FROM ping ORDER BY id;
SELECT * FROM pong ORDER BY id;

DEFINE EVENT log_self ON log WHEN $event = 'CREATE' AND $after.depth < 3 THEN (CREATE log SET depth = $after.depth + 1, parent = $after.id);
CREATE log:root SET depth = 0;
SELECT depth FROM log ORDER BY depth;
