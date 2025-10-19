/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { idx: 'DEFINE INDEX idx ON t COUNT' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "{ building: { initial: 0, pending: 0, status: 'ready', updated: 0 } }"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx', operator: 'Count' }, table: 't' }, operation: 'Iterate Index Count' }, { detail: { idioms: { count: ['count'] }, type: 'Group' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "None"

[[test.results]]
value = "{ building: { initial: 3, pending: 0, status: 'ready', updated: 0 } }"

[[test.results]]
value = "[{ count: 3 }]"

*/
DEFINE INDEX idx ON t COUNT;
INFO FOR TB t;
INFO FOR INDEX idx ON t;
BEGIN;
    CREATE t:1 SET b = true;
    CREATE t:2 SET b = true;
    CREATE t:3 SET b = false;
    CREATE t:4 SET b = false;
    RETURN 'OK';
COMMIT;

SELECT count() FROM t GROUP ALL EXPLAIN;
SELECT count() FROM t GROUP ALL;

DELETE t:2;
SELECT count() FROM t GROUP ALL;

REBUILD INDEX idx ON t;
INFO FOR INDEX idx ON t;
SELECT count() FROM t GROUP ALL;
