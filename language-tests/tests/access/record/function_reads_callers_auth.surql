/**
[env]
namespace = true
database = true
imports = ["access/record/function_reads_callers_auth_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	user: "alice",
}"""

[test]
reason = """
A function body is compiled once and the compile cache serves it to every
principal of the caller's class: every record user of the same access method in
the same database. The session's own values ($auth, $session, $token, $access)
are therefore read when the body runs, never folded into its compiled code, or
the first caller's $auth would select its rows for every later caller.

The harness runs a test as one session, so the second principal is played by
rebinding $auth around the call with a closure parameter, which a function body
reads as it reads the session's own. The body compiled for the first value must
answer the second value's rows. The two-session form of the same check is the
`exec::vm::tests_principal` unit tests.
"""

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "[[doc:1], [doc:2]]"

[[test.results]]
value = "[doc:1]"

*/

RETURN fn::mine();
RETURN [user:alice, user:bob].map(|$auth| fn::mine());
RETURN fn::mine();
