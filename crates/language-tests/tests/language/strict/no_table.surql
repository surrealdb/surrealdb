/**
[env]
namespace = false
database = false
strict = true
clean = true

[test]

[[test.results]]
error = "The namespace 'test' does not exist"

[[test.results]]
error = "Cannot use database without namespace"

[[test.results]]
error = "Specify a namespace to use"

[[test.results]]
error = "Specify a namespace to use"

[[test.results]]
error = "Specify a namespace to use"
*/

USE NAMESPACE test;
USE DATABASE test;
-- DEFINE TABLE test;
DEFINE FIELD extra ON test VALUE true;
CREATE test:tester;
SELECT * FROM test;
