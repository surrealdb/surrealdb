/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:{ val: 123 }, val: 456 }]"

[[test.results]]
value = "[{ id: test:{ val: 123 }, val: 456 }]"

[[test.results]]
value = "[{ id: test:{ val: 123 }, val: 456 }]"

[[test.results]]
value = "[{ id: test:{ val: 123 }, val: 456 }]"

[[test.results]]
value = "[{ id: test:{ val: 123 }, val: 456 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'doc_val', operator: '=', value: 456 }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 1 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'test' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 1 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'rid_val', operator: '=', value: 456 }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 0 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'test' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 0 }, operation: 'Fetch' }]"

*/

DEFINE INDEX doc_val ON test FIELDS val;
DEFINE INDEX rid_val ON test FIELDS id.id().val;

CREATE test:{ val: 123 } SET val = 456;

SELECT * FROM test WHERE val = 456;
SELECT * FROM test WHERE id.val = 456;
SELECT * FROM test WHERE id.id().val = 123;
SELECT * FROM test WHERE record::id(id).val = 123;

SELECT * FROM test WHERE val = 456 EXPLAIN FULL;
SELECT * FROM test WHERE id.val = 456 EXPLAIN FULL;
SELECT * FROM test WHERE id.id().val = 456 EXPLAIN FULL;
SELECT * FROM test WHERE record::id(id).val = 456 EXPLAIN FULL;