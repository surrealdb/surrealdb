/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A field read off a record id held in a parameter works in an IF condition.
"""
issue = 7348

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:test1, test: 'test' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'same'"
*/
DEFINE TABLE test;
CREATE test:test1 SET test = 'test';
LET $test = test:test1;
LET $test_val = 'test';
IF $test.test != $test_val { THROW 'test' };
IF $test.test == $test_val { 'same' };
