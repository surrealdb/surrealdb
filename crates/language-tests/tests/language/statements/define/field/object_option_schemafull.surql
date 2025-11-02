/**
[test]
reason = "Test option<object<schemafull>>"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:1 }]"

[[test.results]]
error = "Found field 'a.x', but no such field exists for table 'test'"

[[test.results]]
value = "[{ id: test:1 }]"

*/

-- Test option<object<schemafull>>
DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD a ON test TYPE option<object<schemafull>>;
DEFINE FIELD a.b ON test TYPE string;

CREATE test:1;
CREATE test:2 SET a = { b: 'hello', x: 'extra' }; -- Should error: field a.x not defined
SELECT * FROM test;

