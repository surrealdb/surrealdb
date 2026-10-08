/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A call after a schema statement of the same block that redefines, removes
	or alters the function reads the definition that statement leaves, never
	the one the block found when it arrived. A function body that is a single
	expression may be substituted for its call when a statement is bound, which
	holds only while no statement of the same block changes the function in
	between: whether the schema statement is the block's own statement, sits
	under a condition read from data, or is repeated by a loop, the call answers
	the new body, or raises once the function is removed.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:0, n: 0 }]"

[[test.results]]
value = "2"

[[test.results]]
value = "3"

[[test.results]]
value = "4"

[[test.results]]
error = "Function 'fn::v' not found: The function 'fn::v' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Function 'fn::w' not found: The function 'fn::w' does not exist"
*/

DEFINE FUNCTION fn::v() { RETURN 1 };
CREATE t:0 SET n = 0;
{ DEFINE FUNCTION OVERWRITE fn::v() { RETURN 2 }; RETURN fn::v() };
{ IF (SELECT VALUE n FROM ONLY t:0) = 0 { DEFINE FUNCTION OVERWRITE fn::v() { RETURN 3 } }; RETURN fn::v() };
{ FOR $i IN 0..2 { DEFINE FUNCTION OVERWRITE fn::v() { RETURN 4 } }; RETURN fn::v() };
{ REMOVE FUNCTION fn::v; RETURN fn::v() };
DEFINE FUNCTION fn::w() { RETURN 1 };
{ IF (SELECT VALUE n FROM ONLY t:0) = 0 { REMOVE FUNCTION fn::w }; RETURN fn::w() };
