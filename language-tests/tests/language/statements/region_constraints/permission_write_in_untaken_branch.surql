/**
[env]
namespace = true
database = true

[test]
reason = "Register #28, row PermissionOrAssertFrame: a write held by a branch of a permission that never runs is refused as a write held by the permission, because the rule reads the code and not the run. The request is refused at plan time."

[test.results]
request-error = "Invalid statement: a write is not allowed in a permission (statement 1)"
*/
DEFINE TABLE person SCHEMAFULL
	PERMISSIONS FOR select WHERE IF false THEN (CREATE audit:1).id != NONE ELSE true END;
