/**
[env]
imports = ["access/record/param_permission_auth_deref_import.surql"]
clean = true

signup = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "admin@example.com",
	admin: true,
}"""

[test]
reason = "A DEFINE PARAM permission predicate that dereferences the auth record ($auth.admin) resolves for a record user, matching the DEFINE FUNCTION path"

[[test.results]]
value = "42"

[[test.results]]
value = "1"

[[test.results]]
value = "7"
*/
RETURN $secret;
RETURN fn::secret();
RETURN $open;
