/**
[env]
new-planner-strategy = "compute-only"

[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'i' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { CancelOnLimit: 3, SkipStart: 2 }, operation: 'StartLimitStrategy' }, { detail: { count: 3 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ b: true, id: i:2 }, { b: true, id: i:3 }, { b: true, id: i:4 }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'i' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { CancelOnLimit: 3 }, operation: 'StartLimitStrategy' }, { detail: { count: 3 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ b: true, id: i:2 }, { b: true, id: i:3 }, { b: true, id: i:4 }]"

*/

{
    CREATE i:0 SET b=true;
    CREATE i:1 SET b=true;
    CREATE i:2 SET b=true;
    CREATE i:3 SET b=true;
    CREATE i:4 SET b=true;
    CREATE i:5 SET b=true;
    RETURN "OK";
};

SELECT * FROM i LIMIT 3 START 2 EXPLAIN FULL;
SELECT * FROM i LIMIT 3 START 2;
SELECT * FROM i WHERE b = true LIMIT 3 START 2 EXPLAIN FULL;
SELECT * FROM i WHERE b = true LIMIT 3 START 2;