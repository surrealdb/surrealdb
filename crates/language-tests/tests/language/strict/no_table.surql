/**
[env]
namespace = false
database = false
strict = true

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'test' does not exist"

[[test.results]]
error = "The table 'test' does not exist"

[[test.results]]
error = "The table 'test' does not exist"
*/

DEFINE NAMESPACE test;
USE NAMESPACE test;
DEFINE DATABASE test;
USE DATABASE test;
-- DEFINE TABLE test;
DEFINE FIELD extra ON test VALUE true;
CREATE test:tester;
SELECT * FROM test;
