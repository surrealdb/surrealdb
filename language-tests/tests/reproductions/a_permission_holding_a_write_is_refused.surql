/**
[env]
namespace = true
database = true

[test]
reason = """
Register #28, row PermissionOrAssertFrame: a statement that writes data is not
allowed in the permission of a table or a field, whichever action the
permission guards and however the write is reached. A permission is evaluated
as a predicate, so a write there is refused when the definition is written and
the request fails as a whole before any statement of it runs. The refusal names
the first such statement of the request, and the statements before it never run.
"""

[test.results]
request-error = "Invalid statement: a write is not allowed in a permission (statement 2)"
*/
DEFINE TABLE ok SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE d SCHEMALESS PERMISSIONS FOR select WHERE (CREATE x:1) != NONE, FOR create, update FULL;
DEFINE FIELD f ON ok TYPE option<int> PERMISSIONS FOR select WHERE (DELETE x) != NONE;
