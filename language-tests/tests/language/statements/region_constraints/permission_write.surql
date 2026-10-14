/**
[env]
namespace = true
database = true

[test]
reason = "Register #28, row PermissionOrAssertFrame: a write is not allowed in the permission of a table. The request is refused at plan time, before any statement of it runs."

[test.results]
request-error = "The PERMISSIONS clause must not have effects: `CREATE` writes data"
*/
DEFINE TABLE p PERMISSIONS FOR select WHERE (CREATE plog:1 RETURN NONE) = [];
