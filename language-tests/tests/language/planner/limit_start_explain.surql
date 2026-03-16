/**
[env]
new-planner-strategy = "compute-only"
timeout = 5000

[test]
reason = "Test LIMIT and START with EXPLAIN plans and ORDER BY"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'item' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { CancelOnLimit: 10, SkipStart: 2 }, operation: 'StartLimitStrategy' }, { detail: { count: 10 }, operation: 'Fetch' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'item2' }, operation: 'Iterate Table' }, { detail: { limit: 12, type: 'MemoryOrderedLimit' }, operation: 'Collector' }]"

[[test.results]]
value = "[1, 2, 3, 4, 5]"

[[test.results]]
value = "[5, 6, 7, 8, 9]"

[[test.results]]
value = "[8, 9, 10]"
*/

CREATE |item:100| RETURN NONE;
SELECT * FROM item LIMIT 10 START 2 EXPLAIN FULL;

CREATE |item2:100| RETURN NONE;
SELECT * FROM item2 ORDER BY id LIMIT 10 START 2 EXPLAIN;

-- LIMIT/START from array (deterministic)
SELECT * FROM [1,2,3,4,5,6,7,8,9,10] LIMIT 5 START 0;
SELECT * FROM [1,2,3,4,5,6,7,8,9,10] LIMIT 5 START 4;
SELECT * FROM [1,2,3,4,5,6,7,8,9,10] LIMIT 5 START 7;
