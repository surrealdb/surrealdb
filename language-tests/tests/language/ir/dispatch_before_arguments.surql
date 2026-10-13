/**
[env]
namespace = true
database = true

[test]
reason = """
A method call on a value resolves the method before its arguments run: an unknown
method raises its own error, and an argument's error surfaces only for a method that exists. A call
through a value that is not a function raises 'not a function' before its argument is evaluated,
whereas a named function evaluates its arguments first. An optional-chain step on NONE, and the right
operand of AND, OR, `??` and `?:`, are not evaluated when the left side decides.
"""

[[test.results]]
value = "true"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
value = "2"

[[test.results]]
value = "5"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "There was a problem running the foo() function. no such method found for the none type"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "There was a problem running the len() function. no such method found for the none type"

[[test.results]]
error = "There was a problem running the foo() function. no such method found for the string type"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "There was a problem running the ANONYMOUS() function. 'int' is not a function"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "There was a problem running the foo() function. no such method found for the none type"

[[test.results]]
value = "NONE"
*/

RETURN true OR (1 + "1");
RETURN 1 ?? (1 + "1");
RETURN 1 ?: (1 + "1");
RETURN NONE ?? 2;
RETURN 0 ?: 5;
RETURN NONE[(1 + "1")];
RETURN [1][(1 + "1")];
RETURN NONE.foo((1 + "1"));
RETURN {a:1}.b[(1 + "1")];
RETURN NONE.len();
RETURN "x".foo((1 + "1"));
RETURN "x".len((1 + "1"));
RETURN fn::missing((1 + "1"));
DEFINE FUNCTION fn::two($a: int, $b: int) { RETURN $a + $b; };
RETURN fn::two((1 + "1"));
RETURN fn::two("a", (1 + "1"));
LET $f = 1;
RETURN $f((1 + "1"));
RETURN string::len("a", (1 + "1"));
RETURN "abc".len((1 + "1"));
LET $b = NONE;
RETURN $b.?.foo((1 + "1"));
RETURN $b.foo((1 + "1"));
RETURN $b.?.x[(1 + "1")];
