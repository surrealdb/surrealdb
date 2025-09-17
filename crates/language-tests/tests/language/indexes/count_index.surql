/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { idx: 'DEFINE INDEX idx ON tbl FIELDS fld COUNT' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx', operator: 'count' }, table: 'tbl' }, operation: 'Count Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

*/
DEFINE INDEX idx ON tbl FIELDS fld COUNT;
INFO FOR TB tbl;
BEGIN;
    CREATE tbl:1 SET fld = 'Counted';
    CREATE tbl:2 SET fld = 'Not counted';
    CREATE tbl:3 SET ign = 'Not counted';
    CREATE tbl:4 SET fld = 'Counted';
    DELETE tbl:2;
    RETURN 'OK';
COMMIT;
SELECT count() FROM tbl GROUP ALL;
SELECT count() FROM tbl GROUP ALL EXPLAIN;