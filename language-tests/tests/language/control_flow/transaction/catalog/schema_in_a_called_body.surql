/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A definition changed inside a called function body is committed with the
	statement that made the call, so the statements after that call see it
	while the call itself ran against the definition in force when it opened.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "0"

[[test.results]]
value = "2"
*/

DEFINE FUNCTION fn::f() { RETURN 1 };
DEFINE FUNCTION fn::redefine() { DEFINE FUNCTION OVERWRITE fn::f() { RETURN 2 }; RETURN 0 };

RETURN fn::f();
RETURN fn::redefine();
RETURN fn::f();
