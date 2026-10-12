/**
[env]
namespace = true
database = true
auth = { level = "owner" }
# The queue entry is keyed outside the namespace subtree, so that an event
# survives the removal of the table it was queued from and can still be drained.
# Nothing drains it here, so it outlives this test's `REMOVE NS`.
clean = true
remote-skip = "asserts that a queued ASYNC event body stays unrun, which holds only because the embedded harness builds its datastores without maintenance tasks; a real server runs them and drains the queue"

[test]
reason = """
	A `DEFINE EVENT ... ASYNC` is queued by the write rather than run by it. The
	statement stores its record, queues the event in its own transaction and
	returns; the body runs out-of-band, under its own retry and nesting budget.
	So a write to a table carrying a firing `ASYNC` event must succeed and must
	stand — neither refused nor rolled back — and none of the body's effects may
	be visible to the statement that fired it. That the entry reaches the queue
	rather than being dropped is not observable from SurrealQL, because nothing
	here drains it; the stage's own tests read the queue back and assert it.

	A synchronous event on the same table is the contrast: its body does run
	inline, for the same three actions, so the `log` table separates "queued"
	from "not fired" rather than leaving both looking alike.

	The harness runs its datastores with no maintenance tasks, so nothing drains
	the queue for the length of this file. That is what makes the assertions on
	`log` deterministic here; it is not a claim that a queued body never runs.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, name: 'a' }]"

[[test.results]]
value = "[{ id: person:1, name: 'a' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: person:1, name: 'b' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, name: 'c' }]"

[[test.results]]
value = "[{ id: log:2, seen: 'UPDATE' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: log:2, seen: 'DELETE' }]"

[[test.results]]
value = "[]"
*/

DEFINE TABLE person SCHEMALESS;
DEFINE TABLE log SCHEMALESS;

-- Fires on every action, and writes `log:1` if it is ever run inline.
DEFINE EVENT audit ON person ASYNC WHEN true THEN (CREATE log:1 SET who = $value.id);

-- The create stands, and the body has not run.
CREATE person:1 SET name = 'a';
SELECT * FROM person;
SELECT * FROM log;

-- So does the update.
UPDATE person:1 SET name = 'b';
SELECT * FROM log;

-- A synchronous event on the same table does run inline, for the same actions.
DEFINE EVENT tally ON person WHEN true THEN (UPSERT log:2 SET seen = $event);

UPDATE person:1 SET name = 'c';
SELECT * FROM log;

-- A delete queues its event too, and the record is still gone.
DELETE person:1;
SELECT * FROM log;
SELECT * FROM person;
