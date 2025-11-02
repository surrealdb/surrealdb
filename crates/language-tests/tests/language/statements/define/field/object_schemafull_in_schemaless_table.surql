/**
[test]
reason = "Test object<schemafull> works in schemaless tables"

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

-- Test object<schemafull> works even in schemaless tables
DEFINE TABLE test; -- schemaless table
DEFINE FIELD a ON test TYPE object<schemafull>;
DEFINE FIELD a.b ON test TYPE number;

CREATE test:1 SET a = { b: 1 };
CREATE test:2 SET a = { b: 1, c: 2 }; -- Should error: field a.c not defined

