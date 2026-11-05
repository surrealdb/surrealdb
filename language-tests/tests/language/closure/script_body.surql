/**
[test]
requires = ["scripting"]
reason = """
A closure whose body calls an embedded script runs it with the closure's
arguments, called directly, handed to a builtin, and invoked from a later
statement.
"""

[[test.results]]
value = "42f"

[[test.results]]
value = "[2f, 3f]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "9f"
*/

RETURN (|$x| function($x) { return arguments[0] * 2; })(21);
RETURN array::map([1, 2], |$v| function($v) { return arguments[0] + 1; });
LET $f = |$x| function($x) { return arguments[0] * 3; };
RETURN $f(3);
