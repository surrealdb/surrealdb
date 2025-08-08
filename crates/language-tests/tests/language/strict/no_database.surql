/**
[env]
database = false
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

USE DB test;
DEFINE TABLE test;
DEFINE FIELD extra ON test VALUE true;
CREATE test:tester;
SELECT * FROM test;
