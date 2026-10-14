/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A RETURN ends the innermost value scope: a block, a subquery, a function or closure body, or a
value-position IF or FOR. A LET whose value is such a scope binds the returned value and answers
NONE, and a RETURN taken inside an operand, an array element, a function argument, an IF condition
or a WHERE block is that scope's value, which the expression around it goes on to use. A block
written as a statement of a block is a value scope too, so `{ { RETURN 1 }; RETURN 2 }` answers 2,
in a function body and in a closure body alike. A statement-level IF or FOR pushes no scope, so its
RETURN passes outwards and ends the top-level statement it stands in; a RETURN that reaches the
top-level statement ends that statement with its value, as in `LET $bare = RETURN 5` and
`WHERE RETURN false`. Parentheses alone open no value scope, so `LET $v = (RETURN 8)` ends its
statement with 8 as `LET $bare = RETURN 5` does. A statement in scalar position that is a value scope takes its own RETURN into
its own destination and resumes after itself.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "8"

[[test.results]]
value = "NONE"

[[test.results]]
value = "6"

[[test.results]]
value = "[1, 11, 3]"

[[test.results]]
value = "1"

[[test.results]]
value = "'a'"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "'if'"

[[test.results]]
value = "2"

[[test.results]]
value = "1"

[[test.results]]
value = "7"

[[test.results]]
value = "[ret_a:3]"

[[test.results]]
value = "5"

[[test.results]]
value = "NONE"

[[test.results]]
value = "false"
*/

LET $d = IF true { RETURN 1 } ELSE { 2 };
RETURN $d;
LET $f = FOR $i IN [1, 2] { RETURN $i };
RETURN $f;
LET $v = (RETURN 8);
RETURN $v;
RETURN { IF true { RETURN 5 }; 6 } + 1;
RETURN [1, { RETURN 11 }, 3];
RETURN math::abs({ RETURN -1; 5 });
RETURN IF ({ RETURN true; false }) { 'a' } ELSE { 'b' };
SELECT VALUE $this FROM [1, 2] WHERE { RETURN false; true };
DEFINE FUNCTION fn::inner() { { RETURN 1 }; RETURN 2 };
RETURN fn::inner();
LET $c = |$a| { { RETURN 1 }; 2 };
RETURN $c(0);
IF true { RETURN 'if' };
IF false { 1 } ELSE IF true { RETURN 2 };
FOR $i IN [1, 2] { RETURN $i };
{ RETURN 7 };
CREATE ret_a:3 RETURN VALUE id;
LET $bare = RETURN 5;
RETURN $bare;
SELECT VALUE $this FROM [1, 2] WHERE RETURN false;
