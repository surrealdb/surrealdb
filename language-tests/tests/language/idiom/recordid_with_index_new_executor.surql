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
value = ''''Project [ctx: Db] {rows: 1, batches: 1}
└────> Scan [ctx: Db] [source: test, predicate: val = 456] {rows: 1, batches: 1}
         actual: [access_method: IndexScan, index: doc_val, access: Equality(456), direction: Forward]

Total rows: 1''''

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1, batches: 1}
└────> Scan [ctx: Db] [source: test, predicate: id.val = 456] {rows: 1, batches: 1}
         actual: [access_method: TableScan, direction: Forward]

Total rows: 1''''

[[test.results]]
value = ''''Project [ctx: Db] {rows: 0, batches: 0}
└────> Scan [ctx: Db] [source: test, predicate: id.id().val = 456] {rows: 0, batches: 0}
         actual: [access_method: IndexScan, index: rid_val, access: Equality(456), direction: Forward]

Total rows: 0''''

[[test.results]]
value = ''''Project [ctx: Db] {rows: 0, batches: 0}
└────> Scan [ctx: Db] [source: test, predicate: record::id(id).val = 456] {rows: 0, batches: 0}
         actual: [access_method: TableScan, direction: Forward]

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
