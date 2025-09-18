/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { idx: 'DEFINE INDEX idx ON t COUNT', idxw: 'DEFINE INDEX idxw ON t COUNT WHERE b = true' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 't' }, operation: 'Index Count' }, { detail: { idioms: { count: ['count'] }, type: 'Group' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 't' }, operation: 'Index Count' }, { detail: { idioms: { count: ['count'] }, type: 'Group' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ b: true, id: t:3 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 2 }]"

*/
DEFINE INDEX idx ON t COUNT;
DEFINE INDEX idxw ON t COUNT WHERE b = true;
INFO FOR TB t;
BEGIN;
    CREATE t:1 SET b = true;
    CREATE t:2 SET b = true;
    CREATE t:3 SET b = false;
    CREATE t:4 SET b = false;
    RETURN 'OK';
COMMIT;
SELECT count() FROM t GROUP ALL EXPLAIN;
SELECT count() FROM t WHERE b = true GROUP ALL EXPLAIN;
SELECT count() FROM t GROUP ALL;
SELECT count() FROM t WHERE b = true GROUP ALL;
UPDATE t:3 SET b = true;
SELECT count() FROM t GROUP ALL;
SELECT count() FROM t WHERE b = true GROUP ALL;
DELETE t:2;
SELECT count() FROM t GROUP ALL;
SELECT count() FROM t WHERE b = true GROUP ALL;