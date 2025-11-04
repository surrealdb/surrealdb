/**
[test]
reason = "Test that object in SCHEMAFULL table is strict by default (no FLEXIBLE)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: { b: 1 }, id: test:1 }]"

[[test.results]]
error = "Found field 'a.c', but no such field exists for table 'test'"

*/

-- Object in SCHEMAFULL table is strict by default
DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD a ON test TYPE object;
DEFINE FIELD a.b ON test TYPE number;

CREATE test:1 SET a = { b: 1 };
CREATE test:2 SET a = { b: 1, c: 2 }; -- Should error: field a.c not defined

