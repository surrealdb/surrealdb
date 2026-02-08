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
value = '''"Project [ctx: Db]\n└────> Filter [ctx: Db] [predicate: num = 123]\n       └────> Scan [ctx: Db] [source: test]\n"'''

[env]
namespace = "test"
database = "test"

*/

OPTION IMPORT;
DEFINE INDEX field_num ON test FIELDS num;
CREATE ONLY test:1 SET num = 123;
SELECT * FROM test WHERE num = 123;
EXPLAIN SELECT * FROM test WHERE num = 123;
