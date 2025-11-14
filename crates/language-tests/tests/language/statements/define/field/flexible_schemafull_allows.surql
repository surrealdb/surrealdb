/**
[test]
reason = "Test that FLEXIBLE in SCHEMAFULL table allows arbitrary nested fields"

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

-- FLEXIBLE in SCHEMAFULL table allows arbitrary nested fields
DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD a ON test TYPE object FLEXIBLE;
DEFINE FIELD a.b ON test TYPE number;

CREATE test:1 SET a = { b: 1 };
CREATE test:2 SET a = { b: 1, c: 2, d: 3 }; -- c and d are allowed

