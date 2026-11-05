/**
[env]
namespace = true
database = true
imports = ["language/types/named/omitted_named_argument_import.surql"]

[test]
reason = "An omitted argument of a function whose parameter is a named type is judged by that type, also where the call is small enough to run in the caller"

[[test.results]]
error = "Incorrect arguments for function fn::f(). Failed to coerce argument `$n`: Expected `posint (int)` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function fn::f(). Failed to coerce argument `$n`: Expected `posint (int)` but found `NONE`"

[[test.results]]
value = "1"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: fn::f()]
    Values [ctx: Rt] [expr: [1]]
''''
*/

RETURN fn::f();
SELECT VALUE fn::f() FROM [1];
RETURN fn::g();
EXPLAIN SELECT VALUE fn::f() FROM [1];
