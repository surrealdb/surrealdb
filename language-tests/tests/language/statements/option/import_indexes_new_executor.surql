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
value = ''''Project [ctx: Db] {rows: 1}
└────> Filter [ctx: Db] [predicate: num = 123] {rows: 1}
       └────> IndexScan [ctx: Db] [index: field_num, access: = 123, direction: Forward] {rows: 1}

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
