/**
[env]
namespace = true
database = true
imports = ["access/record/function_guard_reads_the_callers_access_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "other",
	user: "bob",
}"""

[test]
reason = """
A filter or permission conjunct that reads only `$access` answers the same at every row, so it
is judged at the first row and its verdict held for the rest of the run. Every later row reads
the verdict the first row judged. A session of access method `other` is refused by a function
guard that counts the users visible where `$access = 'user'`, at every call, before and after a
call this session is admitted to, also where the guard runs inside a full-text analyzer. A
function body's filter on `$access` keeps no row, and neither does a table's select permission
on it.

The harness runs each file as one session on a fresh datastore. The warm-cache form, where two
principals of one datastore are judged in turn by the code one of them compiled, is the
`exec::vm::tests_principal::a_function_guard_reads_the_callers_access` unit test.
"""

[[test.results]]
value = "'other'"

[[test.results]]
error = "You don't have permission to run the fn::low function"

[[test.results]]
value = "[]"

[[test.results]]
value = "'other'"

[[test.results]]
error = "You don't have permission to run the fn::low function"

[[test.results]]
error = "You don't have permission to run the fn::low function"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

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
