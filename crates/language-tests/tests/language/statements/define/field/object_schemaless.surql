/**
[test]
reason = "Test plain object (schemaless) allows any nested fields"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: { b: 1 }, id: test:1 }]"

[[test.results]]
value = "[{ a: { b: 1, c: 2, d: 3 }, id: test:2 }]"

*/

-- Test plain object allows any nested fields in SCHEMAFULL table
DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD a ON test TYPE object;
DEFINE FIELD a.b ON test TYPE number;

CREATE test:1 SET a = { b: 1 };
CREATE test:2 SET a = { b: 1, c: 2, d: 3 }; -- c and d are preserved

