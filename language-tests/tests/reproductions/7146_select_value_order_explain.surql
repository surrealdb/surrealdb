/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "SELECT VALUE with ORDER BY and EXPLAIN FULL must not waste work on deferred projection"
issue = 7146

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:a, score: 3 }]"

[[test.results]]
value = "[{ id: test:b, score: 1 }]"

[[test.results]]
value = "[{ id: test:c, score: 2 }]"

*/
DEFINE TABLE OVERWRITE test SCHEMALESS;
INSERT INTO test {id: test:a, score: 3};
INSERT INTO test {id: test:b, score: 1};
INSERT INTO test {id: test:c, score: 2};
