/**
[env]
timeout = 30000
namespace = true
database = true
auth = { level = "owner" }
# The queue entry is keyed outside the namespace subtree, so it outlives the test's `REMOVE NS`.
clean = true
remote-skip = "asserts that a queued ASYNC event body stays unrun, which holds only because the embedded harness builds its datastores without maintenance tasks; a real server runs them and drains the queue"

[test]
reason = """
An `ASYNC` event is queued by a write only when its `WHEN` is truthy. A write whose
action its `WHEN` excludes queues nothing and runs nothing, and a write that stores its
record stands whether or not an event of the table fires. The synchronous events beside
it run inline for the actions they fire for, so the `log` table separates "fired" from
"not fired". The harness runs no maintenance task, so nothing drains the queue and the
assertions on `log` are deterministic.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: aw:1, n: 1 }]"

[[test.results]]
value = "[{ id: aw:1, n: 2 }]"

[[test.results]]
value = "[{ id: aw:2, n: 1 }]"

[[test.results]]
value = "[{ id: aw:2, n: 2 }]"

[[test.results]]
value = "[{ id: aw:1, n: 2 }, { id: aw:2, n: 2 }]"

[[test.results]]
value = "[{ act: 'CREATE', count: 2, tag: 's_live' }, { act: 'UPDATE', count: 2, tag: 's_live' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ act: 'DELETE', count: 1, tag: 's_delete' }, { act: 'CREATE', count: 2, tag: 's_live' }, { act: 'DELETE', count: 1, tag: 's_live' }, { act: 'UPDATE', count: 2, tag: 's_live' }]"
*/

DEFINE TABLE aw SCHEMALESS;
DEFINE TABLE log SCHEMALESS;
-- ASYNC events: never fired by CREATE and UPDATE, and fired by CREATE.
DEFINE EVENT a_delete ON aw ASYNC WHEN $event = "DELETE" THEN (CREATE log SET tag = 'a_delete', act = $event);
DEFINE EVENT a_create ON aw ASYNC WHEN $event = "CREATE" THEN (CREATE log SET tag = 'a_create', act = $event);
-- Synchronous events beside them, which run inline.
DEFINE EVENT s_delete ON aw WHEN $event = "DELETE" THEN (CREATE log SET tag = 's_delete', act = $event);
DEFINE EVENT s_live ON aw WHEN true THEN (CREATE log SET tag = 's_live', act = $event);
CREATE aw:1 SET n = 1;
UPDATE aw:1 SET n = 2;
UPSERT aw:2 SET n = 1;
UPSERT aw:2 SET n = 2;
SELECT * FROM aw ORDER BY id;
SELECT tag, act, count() FROM log GROUP BY tag, act ORDER BY tag, act;
DELETE aw:1;
SELECT tag, act, count() FROM log GROUP BY tag, act ORDER BY tag, act;
