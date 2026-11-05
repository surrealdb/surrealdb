/**
[env]
namespace = true
database = true

[test]
reason = """
Register #28, row PermissionOrAssertFrame: a statement that writes data is not
allowed in the permission of a table or a field, whichever action the
permission guards and however the write is reached. A permission is evaluated
as a predicate, so a write there is refused when the definition is written.
Each definition is a statement of its own: one holding a write fails with the
error naming the write, and the definitions around it are unaffected.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: `CREATE` writes data"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: `DELETE` writes data"
*/
DEFINE TABLE ok SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE d SCHEMALESS PERMISSIONS FOR select WHERE (CREATE x:1) != NONE, FOR create, update FULL;
DEFINE FIELD f ON ok TYPE option<int> PERMISSIONS FOR select WHERE (DELETE x) != NONE;
