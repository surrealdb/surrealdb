/**
[env]
namespace = true
database = true

[test]
reason = "Register #28, row PermissionOrAssertFrame: a write is not allowed in the ASSERT clause of a field. The request is refused at plan time, before any statement of it runs."

[test.results]
request-error = "The ASSERT clause must not have effects: `CREATE` writes data"
*/
DEFINE FIELD f ON t ASSERT (CREATE alog:1 RETURN NONE) = [];
