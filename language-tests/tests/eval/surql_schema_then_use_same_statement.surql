/**
[env.capabilities]
allow-eval-query = ["system"]

[test]
reason = """
A call that may run a schema statement is a change point within its own
statement: a write later in the same statement checks its record against the
field `eval::surql` defined, and a function `eval::surql` defined is called
later in the same statement. Expectations are main's."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `k` of `person:5`: Expected `int` but found `'bad'`"

[[test.results]]
value = "[NONE, 6]"
*/
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD n ON person TYPE int;
RETURN [eval::surql("DEFINE FIELD k ON person TYPE int"), (CREATE person:5 SET n = 5, k = 'bad')];
RETURN [eval::surql("DEFINE FUNCTION fn::thrice($x: int) { RETURN $x * 3 }"), fn::thrice(2)];
