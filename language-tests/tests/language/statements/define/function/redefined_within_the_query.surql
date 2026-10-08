/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A call reads the definition its transaction sees when the call runs. A
	query that defines a function, calls it, redefines it and calls it again
	answers the first body and then the second; a redefinition that fails
	leaves the call reading the definition before it, and a call after the
	function is removed raises. A call made from inside another function's
	body reads the callee the same way, so redefining the callee changes what
	the caller answers.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

[[test.results]]
error = "The function 'fn::foo' already exists"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Function 'fn::foo' not found: The function 'fn::foo' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'first'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'second'"

[[test.results]]
value = "[1, 2]"
*/

DEFINE FUNCTION fn::foo($a: int, $b: int) { RETURN $a + $b };
fn::foo(1, 3);
DEFINE FUNCTION OVERWRITE fn::foo($a: int, $b: int) { RETURN $a * $b };
fn::foo(1, 3);

-- Fails: the function is already defined, so the call still reads the body
-- the OVERWRITE wrote.
DEFINE FUNCTION fn::foo($a: int, $b: int) { RETURN $a - $b };
fn::foo(1, 3);

REMOVE FUNCTION fn::foo;
fn::foo(1, 3);

-- A caller's body reads the callee its transaction sees.
DEFINE FUNCTION fn::inner() { RETURN 'first' };
DEFINE FUNCTION fn::outer() { RETURN fn::inner() };
fn::outer();
DEFINE FUNCTION OVERWRITE fn::inner() { RETURN 'second' };
fn::outer();

-- The same inside one block.
{
	DEFINE FUNCTION fn::bar() { RETURN 1 };
	LET $before = fn::bar();
	DEFINE FUNCTION OVERWRITE fn::bar() { RETURN 2 };
	RETURN [$before, fn::bar()];
};
