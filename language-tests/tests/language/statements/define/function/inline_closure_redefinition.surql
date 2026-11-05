/**
[env]
namespace = true
database = true
auth = { level = "owner" }
imports = ["language/statements/define/function/inline_closure_redefinition_import.surql"]

[test]
reason = """
	A closure runs the definition of the function it calls as it stands when the
	closure is invoked, not as it stood where the closure was written: a function
	redefined between the two is called as redefined.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"
*/

LET $f = || fn::f();
DEFINE FUNCTION OVERWRITE fn::f() { RETURN 2 };
RETURN $f();
