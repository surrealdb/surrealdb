/**
[test]

[[test.results]]
value = "None"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { i: 'DEFINE INDEX i ON b FIELDS t COUNT' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "None"

[[test.results]]
value = "None"

[[test.results]]
value = "None"

[[test.results]]
value = "None"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'i', operator: 'count' }, table: 'b' }, operation: 'Count Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

*/
DEFINE INDEX i ON b FIELDS t COUNT;
INFO FOR TB b;
CREATE b:1 SET t = 'Counted' RETURN NONE;
CREATE b:2 SET t = 'Not counted' RETURN NONE;
CREATE b:3 SET i = 'Not counted' RETURN NONE;
CREATE b:4 SET t = 'Counted' RETURN NONE;
DELETE b:2 RETURN NONE;
SELECT count() FROM b GROUP ALL;
SELECT count() FROM b GROUP ALL EXPLAIN;