/**
[env]
namespace = true
database = true

[test]
reason = """
Releasing a nested savepoint must keep its writes undoable by the enclosing
savepoint. A synchronous DEFINE EVENT nests savepoints: the per-document
savepoint taken for the INSERT is still open when the event's own statement
takes and releases one of its own. When the outer create attempt then fails,
rolling back to the per-document savepoint has to undo everything the attempt
wrote -- the record and its index keys as well as the event's writes.

IGNORE swallows the failure so the transaction commits, which is what makes
the state after the rollback observable.
"""

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
*/

DEFINE TABLE audit SCHEMALESS;
DEFINE TABLE thing SCHEMALESS;
DEFINE EVENT log ON thing WHEN $event = "CREATE" THEN (UPSERT audit:1 SET hits = 1);
INSERT IGNORE INTO thing { id: 1 } RETURN type::int("not a number");
SELECT * FROM thing;
SELECT * FROM audit;
