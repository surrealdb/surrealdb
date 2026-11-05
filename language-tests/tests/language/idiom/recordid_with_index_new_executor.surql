/**
[env]

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
value = '''
'SelectProject [ctx: Db] [projections: *] {rows: 1}
    IndexScan [ctx: Db] [table: test, index: doc_val, access: = 456, direction: Forward] {rows: 1}

Total rows: 1''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *] {rows: 1}
    Filter [ctx: Db] [predicate: id.val = 123] {rows: 1}
        TableScan [ctx: Db] [table: test, direction: Forward] {rows: 1}

Total rows: 1''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *] {rows: 1}
    IndexScan [ctx: Db] [table: test, index: rid_val, access: = 123, direction: Forward] {rows: 1}

Total rows: 1''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *] {rows: 1}
    Filter [ctx: Db] [predicate: record::id(id).val = 123] {rows: 1}
        TableScan [ctx: Db] [table: test, direction: Forward] {rows: 1}

Total rows: 1''''

*/

DEFINE INDEX doc_val ON test FIELDS val;
DEFINE INDEX rid_val ON test FIELDS id.id().val;

CREATE test:{ val: 123 } SET val = 456;

SELECT * FROM test WHERE val = 456;
SELECT * FROM test WHERE id.val = 123;
SELECT * FROM test WHERE id.id().val = 123;
SELECT * FROM test WHERE record::id(id).val = 123;

EXPLAIN ANALYZE SELECT * FROM test WHERE val = 456;
EXPLAIN ANALYZE SELECT * FROM test WHERE id.val = 123;
EXPLAIN ANALYZE SELECT * FROM test WHERE id.id().val = 123;
EXPLAIN ANALYZE SELECT * FROM test WHERE record::id(id).val = 123;
