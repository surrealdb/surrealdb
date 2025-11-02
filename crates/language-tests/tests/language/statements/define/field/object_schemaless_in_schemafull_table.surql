 /**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: { b: 1 }, id: test:1 }]"

[[test.results]]
value = "[{ a: { b: 1 }, id: test:1 }]"

[[test.results]]
value = "[{ a: { b: 1, c: 2 }, id: test:2 }]"

[[test.results]]
value = "[{ a: { b: 1 }, id: test:1 }, { a: { b: 1, c: 2 }, id: test:2 }]"

[[test.results]]
error = "Couldn't coerce value for field `a.b` of `test:3`: Expected `number` but found `NONE`"

[[test.results]]
value = "[{ a: { b: 1 }, id: test:1 }, { a: { b: 1, c: 2 }, id: test:2 }]"

[[test.results]]
error = "Found field 'c', but no such field exists for table 'test'"

[[test.results]]
value = "[{ a: { b: 1 }, id: test:1 }, { a: { b: 1, c: 2 }, id: test:2 }]"

*/

DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD a ON test TYPE object;
DEFINE FIELD a.b ON test TYPE number;

CREATE test:1 SET a = { b: 1 };
SELECT * FROM test;

CREATE test:2 SET a = { b: 1, c: 2 };
SELECT * FROM test;

CREATE test:3 SET a = {}; -- Should error: field a.b not defined
SELECT * FROM test;

CREATE test:4 SET a = {b: 1}, c = 2; -- Should error: field c not defined
SELECT * FROM test;