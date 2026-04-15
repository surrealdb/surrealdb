/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ id: test:1, obj: { a: 1 } }"

[[test.results]]
value = "{ id: test:2, obj: { a: 2, b: 'foo' } }"

[[test.results]]
error = "Couldn't coerce value for field `obj` of `test:3`: Expected `{ a: int, b: none | string }` but found `{ a: 3, b: 'bar', c: 'forbidden' }`"

*/
DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD obj ON test TYPE {
	a: int,
	b: option<string>
};

CREATE ONLY test:1 SET obj = { a: 1 };
CREATE ONLY test:2 SET obj = { a: 2, b: 'foo' };
CREATE ONLY test:3 SET obj = { a: 3, b: 'bar', c: 'forbidden' };
