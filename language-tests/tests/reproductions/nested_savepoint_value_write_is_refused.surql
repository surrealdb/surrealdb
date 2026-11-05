/**
[env]
namespace = true
database = true

[test]
reason = """
A write in a field's VALUE clause is refused when the field is defined, so the
clause cannot nest a savepoint inside the per-document savepoint of an INSERT.
The DEFINE FIELD (statement 5) follows other statements of the request, so it
is planned when it is reached and fails on its own with the clause-effect
error; the statements around it run. The field is not defined, so the INSERT
IGNORE sees no VALUE clause and ignores the duplicate key.
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
error = "The VALUE clause must not have effects: `UPSERT` writes data"

[[test.results]]
value = "[]"
*/

DEFINE TABLE audit SCHEMALESS;
DEFINE TABLE thing SCHEMALESS;
DEFINE INDEX uk ON thing FIELDS k UNIQUE;
CREATE thing:0 SET k = 1;
DEFINE FIELD f ON thing VALUE (UPSERT audit:1 SET hits = 1 RETURN VALUE hits)[0];
INSERT IGNORE INTO thing { id: 1, k: 1 };
