/**
[env]
imports = ["access/record/basic_access_import.surql"]

signup = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "jane.doe@surrealdb.com",
	pass: "hunter2",
}"""

[test]

[[test.results]]
value = "user:dk2k62x05ccfwjuwe8dy"
skip-record-id-key = true


[[test.results]]
value = "{ AC: 'account', DB: 'test', NS: 'test', iss: 'SurrealDB' }"

[[test.results]]
value = "'account'"

[[test.results]]
value = "{ ac: 'account', db: 'test', id: NONE, ip: NONE, ns: 'test', or: NONE }"

[[test.results]]
value = "true"
*/
$auth;
$token.{ AC, DB, NS, iss };
$access;
$session.{ ac, db, id, ip, ns, or};
$session.tk == $token
