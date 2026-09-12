/**
[env]
namespace = true
database = true

[test]
reason = """
A block nested inside another block is a value step of the enclosing block:
its `LET` bindings are scoped to the inner block and do not leak outward, and
its own value — the last statement it runs — is the running value of the
enclosing block. Only a `LET` directly in a block binds for the statements
that follow it there.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "5"
*/

RETURN { { LET $x = 1 }; RETURN $x };
RETURN { { LET $x = 1; $x + 1 } };
RETURN { { LET $x = 1; $x + 1 }; $x };
DEFINE FUNCTION fn::leak() { { LET $x = 1 }; RETURN $x; };
RETURN fn::leak();
RETURN { LET $y = 5; { LET $y = 6 }; $y };
