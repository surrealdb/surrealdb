/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ id: test:1 }"

[[test.results]]
error = "Couldn't coerce value for field `obj.a` of `test:2`: Expected `string` but found `NONE`"

[[test.results]]
value = "{ id: test:3, obj: { a: 'abc' } }"

*/
DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD obj ON test TYPE option<object>;
DEFINE FIELD obj.a ON test TYPE string;

CREATE ONLY test:1;
CREATE ONLY test:2 SET obj = {};
CREATE ONLY test:3 SET obj = { a: 'abc' };
