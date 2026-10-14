/**
[env]
namespace = true
database = true

[test]
reason = "A LET of a name the system binds is refused as a LET of a protected name is: `$this`, `$self`, `$parent`, `$value`, `$input`, `$before`, `$after` and `$event` are values a clause, an event or a field definition reads from the frame it runs in, so no statement binds one, whether it is a whole statement, written in a block or a branch, typed, in a function body, which `DEFINE FUNCTION` refuses so that no function is defined, or in a closure body. The refusal is the whole statement's and no binding is made. An ordinary name is bound."

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'self' is a protected variable and cannot be set"

[[test.results]]
error = "'parent' is a protected variable and cannot be set"

[[test.results]]
error = "'value' is a protected variable and cannot be set"

[[test.results]]
error = "'input' is a protected variable and cannot be set"

[[test.results]]
error = "'before' is a protected variable and cannot be set"

[[test.results]]
error = "'after' is a protected variable and cannot be set"

[[test.results]]
error = "'event' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'self' is a protected variable and cannot be set"

[[test.results]]
error = "'parent' is a protected variable and cannot be set"

[[test.results]]
error = "'value' is a protected variable and cannot be set"

[[test.results]]
error = "'input' is a protected variable and cannot be set"

[[test.results]]
error = "'before' is a protected variable and cannot be set"

[[test.results]]
error = "'after' is a protected variable and cannot be set"

[[test.results]]
error = "'event' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'self' is a protected variable and cannot be set"

[[test.results]]
error = "'parent' is a protected variable and cannot be set"

[[test.results]]
error = "'value' is a protected variable and cannot be set"

[[test.results]]
error = "'input' is a protected variable and cannot be set"

[[test.results]]
error = "'before' is a protected variable and cannot be set"

[[test.results]]
error = "'after' is a protected variable and cannot be set"

[[test.results]]
error = "'event' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'self' is a protected variable and cannot be set"

[[test.results]]
error = "'parent' is a protected variable and cannot be set"

[[test.results]]
error = "'value' is a protected variable and cannot be set"

[[test.results]]
error = "'input' is a protected variable and cannot be set"

[[test.results]]
error = "'before' is a protected variable and cannot be set"

[[test.results]]
error = "'after' is a protected variable and cannot be set"

[[test.results]]
error = "'event' is a protected variable and cannot be set"

[[test.results]]
error = "'value' is a protected variable and cannot be set"

[[test.results]]
error = "Function 'fn::let_value' not found: The function 'fn::let_value' does not exist"

[[test.results]]
error = "'event' is a protected variable and cannot be set"

[[test.results]]
error = "Function 'fn::let_event' not found: The function 'fn::let_event' does not exist"

[[test.results]]
error = "'input' is a protected variable and cannot be set"

[[test.results]]
value = "7"

*/

LET $this = 1;
LET $self = 1;
LET $parent = 1;
LET $value = 1;
LET $input = 1;
LET $before = 1;
LET $after = 1;
LET $event = 1;
{ LET $this = 1; $this };
{ LET $self = 1; $self };
{ LET $parent = 1; $parent };
{ LET $value = 1; $value };
{ LET $input = 1; $input };
{ LET $before = 1; $before };
{ LET $after = 1; $after };
{ LET $event = 1; $event };
LET $this: int = 1;
LET $self: int = 1;
LET $parent: int = 1;
LET $value: int = 1;
LET $input: int = 1;
LET $before: int = 1;
LET $after: int = 1;
LET $event: int = 1;
IF true { LET $this = 1; };
IF true { LET $self = 1; };
IF true { LET $parent = 1; };
IF true { LET $value = 1; };
IF true { LET $input = 1; };
IF true { LET $before = 1; };
IF true { LET $after = 1; };
IF true { LET $event = 1; };
DEFINE FUNCTION fn::let_value() { LET $value = 1; RETURN $value; };
RETURN fn::let_value();
DEFINE FUNCTION fn::let_event() { LET $event = 1; RETURN $event; };
RETURN fn::let_event();
RETURN (|$e| { LET $input = $e; $input })(1);
{ LET $other = 7; $other };
