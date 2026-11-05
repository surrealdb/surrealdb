/**
[env]
imports = ["access/record/param_permission_auth_deref_import.surql"]
clean = true

signup = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "user@example.com",
	admin: false,
}"""

[test]
reason = "A record user failing a DEFINE PARAM permission predicate is denied (ParamPermissions), not surfaced as a DatabaseEmpty error"

[[test.results]]
error = "You don't have permission to view the $secret parameter"

[[test.results]]
error = "You don't have permission to run the fn::secret function"

[[test.results]]
value = "7"
*/
RETURN $secret;
RETURN fn::secret();
RETURN $open;
