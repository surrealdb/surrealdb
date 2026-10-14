/**
[env]
namespace = true
database = true

[test]
reason = """
A write in a field's VALUE clause is refused when the field is defined, so the
clause cannot nest a savepoint inside the per-document savepoint of an INSERT.
The request is refused at plan time, at the DEFINE FIELD (statement 5), before
any statement of it runs: none of the statements around it executes, and no
state is written.
"""

[test.results]
request-error = "Invalid statement: a write is not allowed in a VALUE clause (statement 5)"
*/

DEFINE TABLE audit SCHEMALESS;
DEFINE TABLE thing SCHEMALESS;
DEFINE INDEX uk ON thing FIELDS k UNIQUE;
CREATE thing:0 SET k = 1;
DEFINE FIELD f ON thing VALUE (UPSERT audit:1 SET hits = 1 RETURN VALUE hits)[0];
INSERT IGNORE INTO thing { id: 1, k: 1 };
