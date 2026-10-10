/**
[env]
namespace = true
database = true

[test]
reason = """
Releasing a nested savepoint must keep its writes undoable by the enclosing
savepoint. A field's VALUE clause that writes nests savepoints: the
per-document savepoint taken for the INSERT is still open when the clause's
UPSERT takes and settles one of its own. When the outer create attempt then
fails on a unique-index conflict, rolling back to the per-document savepoint
has to undo everything the attempt wrote -- the record and its index keys as
well as the clause's write.

IGNORE skips the conflict so the transaction commits, which is what makes the
state after the rollback observable. The final insert succeeds, showing the
clause does write when the attempt stands.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: thing:0, k: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: thing:0, k: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ hits: 1, id: audit:1 }]"
*/

DEFINE TABLE audit SCHEMALESS;
DEFINE TABLE thing SCHEMALESS;
DEFINE INDEX uk ON thing FIELDS k UNIQUE;
CREATE thing:0 SET k = 1;
DEFINE FIELD f ON thing VALUE (UPSERT audit:1 SET hits = 1 RETURN VALUE hits)[0];
INSERT IGNORE INTO thing { id: 1, k: 1 };
SELECT * FROM thing;
SELECT * FROM audit;
INSERT IGNORE INTO thing { id: 2, k: 2 } RETURN NONE;
SELECT * FROM audit;
