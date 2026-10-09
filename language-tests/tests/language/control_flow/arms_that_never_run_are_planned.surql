/**
[env]
namespace = true
database = true

[test]
reason = """
An IF arm that never runs, whether a literal or a constant LET rules it out,
still raises the refusal of a LET of a protected name it holds, as the
statement's error. An arm that never runs and holds no such LET changes no
answer.
"""

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "'session' is a protected variable and cannot be set"

[[test.results]]
value = "2"

[[test.results]]
value = "2"
*/
{ LET $x = false; IF $x { LET $auth = 1 }; RETURN 2 };
IF false { LET $auth = 1 } ELSE { 2 };
IF true { 1 } ELSE { LET $session = 1 };
{ LET $x = false; IF $x { 1 } ELSE { 2 } };
IF false { 1 } ELSE { 2 };
