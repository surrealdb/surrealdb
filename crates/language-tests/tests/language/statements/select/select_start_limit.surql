/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'i' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { CancelOnLimit: 2, SkipStart: 1 }, operation: 'StartLimitStrategy' }, { detail: { count: 2 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ b: true, id: i:1 }, { b: true, id: i:2 }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'i' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { CancelOnLimit: 2 }, operation: 'StartLimitStrategy' }, { detail: { count: 2 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ b: true, id: i:1 }, { b: true, id: i:2 }]"

*/

BEGIN;
    CREATE i:0 SET b=true;
    CREATE i:1 SET b=true;
    CREATE i:2 SET b=true;
    CREATE i:3 SET b=true;
    RETURN "OK";
COMMIT;

SELECT * FROM i LIMIT 2 START 1 EXPLAIN FULL;
SELECT * FROM i LIMIT 2 START 1;
SELECT * FROM i WHERE b = true LIMIT 2 START 1 EXPLAIN FULL;
SELECT * FROM i WHERE b = true LIMIT 2 START 1;