/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A `LET` of a protected name is refused wherever it is written in a statement,
including an `IF` arm that never runs: a condition a literal or a constant
`LET` settles false, and the `ELSE` after a condition settled true, are planned
like any other arm, so the statement holding them fails as main fails it. A
A statement with no protected `LET` in it runs.
"""

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
value = "2"

[[test.results]]
value = "3"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
value = "5"
*/

{ LET $x = false; IF $x { LET $auth = 1 }; RETURN 2 };
{ LET $x = 1; IF $x { LET $auth = 1 }; RETURN 2 };
IF false { LET $auth = 1 }; RETURN 2;
{ LET $x = false; IF $x { LET $z = 1 } ELSE { RETURN 3 }; RETURN 2 };
IF true { RETURN 1 } ELSE { LET $auth = 1 };
{ LET $x = true; IF $x { RETURN 1 } ELSE { LET $auth = 1 } };
{ LET $x = false; IF $x { LET $z = 1 } ELSE { LET $auth = 1 }; RETURN 2 };
{ LET $x = 'yes'; IF $x { RETURN 5 }; RETURN 2 };
