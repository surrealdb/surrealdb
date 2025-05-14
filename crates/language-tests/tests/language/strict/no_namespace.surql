/**
[env]
strict = true
*/

-- DEFINE NAMESPACE test;
DEFINE DATABASE test;
DEFINE TABLE test;
DEFINE FIELD extra ON test VALUE true;
CREATE test:tester;
SELECT * FROM test;
