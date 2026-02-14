/**

[test]
issue = 5581
reason = "Check that SELECT COUNT() with index uses the right iteration operation"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = '''"Aggregate [ctx: Db] [mode: GROUP ALL] {rows: 1, batches: 1}
└────> Scan [ctx: Db] [source: test, predicate: indexedField = 'cake'] {rows: 10, batches: 1}
         actual: [access_method: IndexScan, index: idx_indexedField, access: Equality('cake'), direction: Forward]

Total rows: 1"'''

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = '''"Aggregate [ctx: Db] [mode: GROUP ALL] {rows: 1, batches: 1}
└────> Scan [ctx: Db] [source: test, predicate: indexedField = 'cake' AND unindexedField = false] {rows: 0, batches: 0}
         actual: [access_method: IndexScan, index: idx_indexedField, access: Equality('cake'), direction: Forward]

Total rows: 1"'''

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

*/

DEFINE INDEX idx_indexedField ON test FIELDS indexedField;
CREATE |test:10| CONTENT { indexedField: 'cake', unindexedField: true} RETURN NONE;

EXPLAIN ANALYZE SELECT COUNT() FROM test WHERE indexedField = 'cake' GROUP ALL;
SELECT COUNT() FROM test WITH NOINDEX WHERE indexedField = 'cake' GROUP ALL;
SELECT COUNT() FROM test WHERE indexedField = 'cake' GROUP ALL;

EXPLAIN ANALYZE SELECT COUNT() FROM test WHERE indexedField = 'cake' AND unindexedField = false GROUP ALL;
SELECT COUNT() FROM test WITH NOINDEX WHERE indexedField = 'cake' AND unindexedField = false GROUP ALL;
SELECT COUNT() FROM test WHERE indexedField = 'cake' AND unindexedField = false GROUP ALL;
