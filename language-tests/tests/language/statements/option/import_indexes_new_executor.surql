/**
[test]
reason = "Test that OPTION IMPORT populates indexes correctly"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ id: test:1, num: 123 }"

[[test.results]]
value = "[{ id: test:1, num: 123 }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1, batches: 1}
└────> Scan [ctx: Db] [source: test, predicate: num = 123] {rows: 1, batches: 1}
         actual: [access_method: IndexScan, index: field_num, access: Equality(123), direction: Forward]

Total rows: 1''''

[env]
namespace = "test"
database = "test"

*/

OPTION IMPORT;
DEFINE INDEX field_num ON test FIELDS num;
CREATE ONLY test:1 SET num = 123;
SELECT * FROM test WHERE num = 123;
EXPLAIN ANALYZE SELECT * FROM test WHERE num = 123;
