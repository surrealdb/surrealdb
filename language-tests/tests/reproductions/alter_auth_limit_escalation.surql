/**
[env]
namespace = true
database = true
imports = ["reproductions/alter_auth_limit_escalation_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	user: "db_owner",
	pass: "db_owner",
}"""

[test]
reason = "ALTER FUNCTION must recompute auth_limit from the altering principal. A root-defined function altered by a DB-owner must not allow namespace-level operations."

[[test.results]]
error = false

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

*/

-- DB-owner alters the root-defined function to attempt namespace-level escalation.
-- The auth_limit is recomputed from the DB-owner's actual privileges on ALTER.
ALTER FUNCTION fn::escalate() {
	DEFINE USER escalated ON NAMESPACE PASSWORD 'pass' ROLES OWNER;
};

-- Calling the altered function must fail: the auth_limit is now DB-level,
-- blocking the embedded DEFINE USER ON NAMESPACE.
fn::escalate();
