/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["compute-only"]

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

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'test' }, operation: 'Iterate Table' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 3 }, operation: 'Fetch' }]"
*/

DEFINE TABLE OVERWRITE test SCHEMALESS;
INSERT INTO test {id: test:a, score: 3};
INSERT INTO test {id: test:b, score: 1};
INSERT INTO test {id: test:c, score: 2};

-- EXPLAIN FULL should produce explanation output without running project_value
SELECT VALUE score FROM test ORDER BY score EXPLAIN FULL;
