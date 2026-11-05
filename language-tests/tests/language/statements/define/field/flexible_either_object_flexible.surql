/**
[test]
reason = "Field-wide FLEXIBLE relaxes object arms in a union type"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:1, val: 1 }]"

[[test.results]]
value = "[{ id: test:2, val: { extra: true } }]"

[[test.results]]
value = "[{ id: test:3, val: 'a string' }]"

[[test.results]]
error = "Couldn't coerce value for field `val` of `test:4`: Expected `int | object | string` but found `true`"

*/

DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD val ON test TYPE int | object | string FLEXIBLE;

CREATE test:1 SET val = 1;
CREATE test:2 SET val = { extra: true };
CREATE test:3 SET val = "a string";
CREATE test:4 SET val = true;
