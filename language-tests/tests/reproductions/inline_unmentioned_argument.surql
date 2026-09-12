/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A call evaluates and coerces every argument before the body runs, so an
	argument the body never mentions still raises when evaluating it does.
	Replacing the call by its body drops such an argument, and is done only
	when evaluating it cannot raise. Both engines agree on every strategy.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "Could not cast into `int` using input `NONE`"

[[test.results]]
value = "1"

[[test.results]]
value = "1"
*/

DEFINE FUNCTION fn::one($x: any) { RETURN 1; };
RETURN fn::one(<int> NONE);
RETURN fn::one(1);
RETURN fn::one(1 + 1);
