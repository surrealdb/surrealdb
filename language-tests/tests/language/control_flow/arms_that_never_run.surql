/**
[env]
namespace = true
database = true

[test]
reason = """
A LET no statement may hold — of a protected name, or written as an expression —
is refused from the text written, so an IF arm or a loop body that never runs
still fails the statement holding it, whether a literal, a constant LET or the
data rules it out, and whether or not a pass folds the arm away. That holds for
a LET in the body of a closure the arm writes, too. An arm that never runs and
holds no such LET changes no answer.
"""

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "'session' is a protected variable and cannot be set"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
value = "2"

[[test.results]]
value = "2"

[[test.results]]
value = "2"

[[test.results]]
value = "2"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"
*/
{ LET $x = false; IF $x { LET $auth = 1 }; RETURN 2 };
IF false { LET $auth = 1 } ELSE { 2 };
IF true { 1 } ELSE { LET $session = 1 };
IF false { SELECT * FROM t WHERE (LET $x = 1) } ELSE { 2 };
{ LET $x = false; IF $x { SELECT * FROM t WHERE (LET $x = 1) } ELSE { 2 } };
RETURN IF false THEN { LET $auth = 1; 1 } ELSE 2 END;
{ LET $f = false; RETURN IF $f THEN { LET $auth = 1; 1 } ELSE 2 END };
FOR $i IN [] { LET $auth = 1 };
{ DEFINE TABLE later; LET $x = false; IF $x { LET $auth = 1 }; RETURN 2 };
{ LET $x = false; IF $x { 1 } ELSE { 2 } };
IF false { 1 } ELSE { 2 };
IF false { SELECT * FROM t WHERE a @@ 'x' } ELSE { 2 };
{ LET $x = false; IF $x { SELECT * FROM t WHERE a @@ 'x' } ELSE { 2 } };
IF false { LET $f = || { LET $auth = 2 } } ELSE { 1 };
