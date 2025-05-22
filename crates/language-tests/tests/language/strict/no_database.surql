/**
[env]
strict = true

[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "The database 'test' does not exist"

[[test.results]]
error = "The database 'test' does not exist"

[[test.results]]
error = "The database 'test' does not exist"

[[test.results]]
error = "The database 'test' does not exist"
*/

DEFINE NAMESPACE test;
-- DEFINE DATABASE test;
DEFINE TABLE test;
DEFINE FIELD extra ON test VALUE true;
CREATE test:tester;
SELECT * FROM test;
