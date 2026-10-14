/**
[env]
namespace = true
database = true

[test]
reason = "A LET of `$this` is refused as a LET of a protected name is: at the top level, in a block, in a function body, which `DEFINE FUNCTION` refuses so that no function is defined, and in a closure body. `$this` names the value a clause or an element filter reads, so no statement binds it. `$that` is an ordinary name."

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "Function 'fn::let_this' not found: The function 'fn::let_this' does not exist"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
value = "7"

*/

LET $this = 1;
{ LET $this = 1; $this };
DEFINE FUNCTION fn::let_this() { LET $this = 1; RETURN $this; };
RETURN fn::let_this();
RETURN (|$e| { LET $this = $e; $this })(1);
{ LET $that = 7; $that };
