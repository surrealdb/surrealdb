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
value = ''''Project [ctx: Db] {rows: 1}
  Filter [ctx: Db] [predicate: val = 456] {rows: 1}
    IndexScan [ctx: Db] [index: doc_val, access: = 456, direction: Forward] {rows: 1}

Total rows: 1''''

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1}
  TableScan [ctx: Db] [table: test, direction: Forward, predicate: id.val = 456] {rows: 1}

Total rows: 1''''

[[test.results]]
value = ''''Project [ctx: Db] {rows: 0}
  Filter [ctx: Db] [predicate: id.id().val = 456] {rows: 0}
    IndexScan [ctx: Db] [index: rid_val, access: = 456, direction: Forward] {rows: 0}

Total rows: 0''''

[[test.results]]
value = ''''Project [ctx: Db] {rows: 0}
  TableScan [ctx: Db] [table: test, direction: Forward, predicate: record::id(id).val = 456] {rows: 0}

Total rows: 0''''

*/

DEFINE INDEX doc_val ON test FIELDS val;
DEFINE INDEX rid_val ON test FIELDS id.id().val;

CREATE test:{ val: 123 } SET val = 456;

SELECT * FROM test WHERE val = 456;
SELECT * FROM test WHERE id.val = 456;
SELECT * FROM test WHERE id.id().val = 123;
SELECT * FROM test WHERE record::id(id).val = 123;

EXPLAIN ANALYZE SELECT * FROM test WHERE val = 456;
EXPLAIN ANALYZE SELECT * FROM test WHERE id.val = 456;
EXPLAIN ANALYZE SELECT * FROM test WHERE id.id().val = 456;
EXPLAIN ANALYZE SELECT * FROM test WHERE record::id(id).val = 456;
