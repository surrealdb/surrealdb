/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A statement in a later transaction sees every definition committed before
	that transaction opened: those of the request's own earlier statements.
	Here a function is defined, overwritten, altered and removed between calls,
	and each call sees the definition in force when its statement opens.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
value = "NONE"

[[test.results]]
value = "6"

[[test.results]]
value = "[{ id: t:1, v: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[12]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Function 'fn::f' not found: The function 'fn::f' does not exist"
*/

DEFINE FUNCTION fn::f($x: int) { RETURN $x * 2 };
RETURN fn::f(2);

DEFINE FUNCTION OVERWRITE fn::f($x: int) { RETURN $x * 3 };
RETURN fn::f(2);

CREATE t:1 SET v = fn::f(1);

ALTER FUNCTION fn::f($x: int) { RETURN $x * 4 };
SELECT VALUE fn::f(v) FROM t;

REMOVE FUNCTION fn::f;
RETURN fn::f(2);
