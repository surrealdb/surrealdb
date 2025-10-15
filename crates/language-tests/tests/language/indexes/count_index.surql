/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { idx: 'DEFINE INDEX idx ON t COUNT' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "{ building: { status: 'ready' } }"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx', operator: 'Count' }, table: 't' }, operation: 'Iterate Index Count' }, { detail: { idioms: { count: ['count'] }, type: 'Group' }, operation: 'Collector' }]"

[[_test.results]]
value = "[{ detail: { plan: { index: 'idxw', operator: 'Count', where: 'WHERE b = true' }, table: 't' }, operation: 'Iterate Index Count' }, { detail: { idioms: { count: ['count'] }, type: 'Group' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ count: 4 }]"

[[_test.results]]
value = "[{ count: 2 }]"

[[_test.results]]
value = "[{ b: true, id: t:3 }]"

[[_test.results]]
value = "[{ count: 4 }]"

[[_test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 3 }]"

[[_test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "None"

[[test.results]]
value = "None"

[[test.results]]
value = "{ building: { status: 'ready' } }"

[[_test.results]]
value = "{}"

[[test.results]]
value = "[{ count: 3 }]"

[[_test.results]]
value = "[{ count: 2 }]"

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

REMOVE INDEX idx ON t;
DEFINE INDEX idx ON t COUNT;
INFO FOR INDEX idx ON t;
SELECT count() FROM t GROUP ALL;
