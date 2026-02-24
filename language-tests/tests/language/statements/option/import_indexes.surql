/**
[env]
namespace = "test"
database = "test"
new-planner-strategy = "compute-only"

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
value = "[{ detail: { plan: { index: 'field_num', operator: '=', value: 123 }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

*/

OPTION IMPORT;
DEFINE INDEX field_num ON test FIELDS num;
CREATE ONLY test:1 SET num = 123;
SELECT * FROM test WHERE num = 123;
SELECT * FROM test WHERE num = 123 EXPLAIN;

