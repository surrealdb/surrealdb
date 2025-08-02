/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "42"

[[test.results]]
error = "Tried to set `$bar`, but couldn't coerce value: Expected `int` but found `'hello'`"

[[test.results]]
value = "NONE"

*/

LET $foo: int = 42;
RETURN $foo;
LET $bar: int = 'hello';
RETURN $bar;
