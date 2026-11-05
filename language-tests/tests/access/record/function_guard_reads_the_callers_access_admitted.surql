/**
[env]
namespace = true
database = true
imports = ["access/record/function_guard_reads_the_callers_access_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "user",
	user: "alice",
}"""

[test]
reason = """
The admitted side of `function_guard_reads_the_callers_access_denied.surql`: a session of
access method `user` is admitted by the guard that counts the users visible where
`$access = 'user'`, at every call, before and after a call this session is refused, and a
function body's filter on `$access` and a table's select permission on it keep every row.
"""

[[test.results]]
value = "'user'"

[[test.results]]
value = "'one'"

[[test.results]]
value = "[user:alice, user:bob, user:carol]"

[[test.results]]
error = "You don't have permission to run the fn::other function"

[[test.results]]
value = "'one'"

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "[user:alice, user:bob, user:carol]"

[[test.results]]
value = "[gated:1, gated:2, gated:3]"

[[test.results]]
value = "[mixed:1, mixed:2, mixed:3]"

*/

RETURN $access;
RETURN fn::low('ONE');
RETURN fn::seen();
RETURN fn::other();
RETURN fn::low('ONE');
RETURN SELECT VALUE id FROM doc WHERE txt @@ 'ONE';
RETURN fn::seen();
RETURN SELECT VALUE id FROM gated;
RETURN SELECT VALUE id FROM mixed;
