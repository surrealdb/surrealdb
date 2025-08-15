/**
[env]
database = false
strict = true
clean = true

[test]

[[test.results]]
error = "The database 'test' does not exist"

[[test.results]]
error = "Specify a database to use"

[[test.results]]
error = "Specify a database to use"

[[test.results]]
error = "Specify a database to use"

[[test.results]]
error = "Specify a database to use"
*/

USE DB test;
DEFINE TABLE test;
DEFINE FIELD extra ON test VALUE true;
CREATE test:tester;
SELECT * FROM test;
