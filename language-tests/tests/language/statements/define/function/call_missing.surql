/**
[env]
namespace = true
database = true

[test]
reason = """
Calling a function the database does not define fails with
`Function '<name>' not found: <reason>`, wherever the call is reached: at the
top level, per row, and from another function's body. Client SDKs match on
this wording."""

[[test.results]]
error = "Function 'fn::nope' not found: The function 'fn::nope' does not exist"

[[test.results]]
error = "Function 'fn::nope' not found: The function 'fn::nope' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Function 'fn::nope' not found: The function 'fn::nope' does not exist"

[[test.results]]
value = "[{ id: x:1 }]"

[[test.results]]
error = "Function 'fn::nope' not found: The function 'fn::nope' does not exist"
*/

RETURN fn::nope();
SELECT fn::nope() FROM [1];
DEFINE FUNCTION fn::a() { RETURN fn::nope() };
RETURN fn::a();
CREATE x:1;
SELECT fn::nope() FROM x;
