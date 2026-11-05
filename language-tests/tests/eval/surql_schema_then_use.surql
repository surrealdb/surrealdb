/**
[env]
compatibility = 3

[env.capabilities]
allow-eval-query = ["system"]

[test]
reason = """
A schema statement `eval::surql` runs takes effect for what follows it: the
next statement, a later statement of the same block, a later element of the
same expression, and a function it defines called by the next statement."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `n` of `person:1`: Expected `int` but found `'x'`"

[[test.results]]
value = "[{ id: person:2, n: 2 }]"

[[test.results]]
error = "Couldn't coerce value for field `m` of `person:3`: Expected `string` but found `4`"

[[test.results]]
value = "[{ id: person:4, m: 'ok', n: 4 }]"

[[test.results]]
value = "[NONE, 'DEFINE TABLE fresh TYPE NORMAL SCHEMAFULL PERMISSIONS NONE']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "42"

[[test.results]]
value = "[{ id: person:2, n: 2 }, { id: person:4, m: 'ok', n: 4 }]"
*/
DEFINE TABLE person SCHEMAFULL;
RETURN eval::surql("DEFINE FIELD n ON person TYPE int");
CREATE person:1 SET n = "x";
CREATE person:2 SET n = 2;
{ eval::surql("DEFINE FIELD m ON person TYPE string"); CREATE person:3 SET n = 3, m = 4; };
{ eval::surql("DEFINE FIELD m ON person TYPE string"); CREATE person:4 SET n = 4, m = 'ok'; };
RETURN [eval::surql("DEFINE TABLE fresh SCHEMAFULL"), (INFO FOR DB).tables.fresh];
RETURN eval::surql("DEFINE FUNCTION fn::twice($x: int) { RETURN $x * 2 }");
RETURN fn::twice(21);
SELECT * FROM person ORDER BY id;
