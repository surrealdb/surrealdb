/**
[env]
namespace = true
database = true

[test]
reason = "LET fallback in FOR loop must enforce protected-parameter validation and kind coercion"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "Tried to set `$x`, but couldn't coerce value: Expected `int` but found `{ id: person:b, name: 'test' }`"

*/

-- Protected parameter inside FOR (uses CREATE to force planner fallback)
FOR $i IN [1] {
	LET $auth = CREATE ONLY person:a SET name = 'test';
};

-- Typed LET with coercion failure inside FOR (CREATE returns an object, not int)
FOR $i IN [1] {
	LET $x: int = CREATE ONLY person:b SET name = 'test';
};
