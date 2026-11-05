/**
[test]
reason = """
	An `ASYNC RETRY` event's queue entry is resolved in a transaction of its
	own after its body's transaction ends: an attempt that loses its commit is
	requeued with its attempt count raised, from a fresh transaction rather
	than the finished one, and an entry that another consumer has already run
	is left gone. So a write burst against a table carrying such events neither
	strands an entry nor reports `Couldn't update a finished transaction`. The
	queue is drained by a maintenance task the runner does not start, so the
	draining is pinned by the tests in `surrealdb-core`'s `doc::event` and
	`it::asyncevent`. This file holds the writes, which succeed and are not
	held up by the events they queue.
"""
issue = 7411

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 0, b: 0, id: sink:c }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "20"

[[test.results]]
value = "{ a: 0, b: 0 }"

[env]
namespace = true
database = true
auth = { level = "owner" }
clean = true
remote-skip = "asserts that queued ASYNC event bodies stay unrun, which holds only because the embedded harness builds its datastores without maintenance tasks"
*/
DEFINE TABLE t SCHEMALESS;
DEFINE TABLE sink SCHEMALESS;
CREATE sink:c SET a = 0, b = 0;
DEFINE EVENT e1 ON TABLE t WHEN true THEN { UPDATE sink:c SET a += 1; } ASYNC RETRY 3;
DEFINE EVENT e2 ON TABLE t WHEN true THEN { UPDATE sink:c SET b += 1; } ASYNC RETRY 3;
FOR $i IN 0..20 { LET $r = type::record("t", $i); UPSERT $r SET v = $i; UPDATE $r SET v = $i + 1; };
count(SELECT * FROM t);
SELECT a, b FROM ONLY sink:c;
