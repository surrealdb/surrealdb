/**
[env]
namespace = "test"
database = "test"

[test]
reason = "Test that OPTION IMPORT populates indexes correctly"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:1, num: 123 }]"

*/
OPTION IMPORT;
DEFINE INDEX field_num ON test FIELDS num;
CREATE ONLY test:1 SET num = 123;
SELECT * FROM test WHERE num = 123;
