/**
[test]
reason = "Test FLEXIBLE with option<object>"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:1 }]"

[[test.results]]
value = "[{ a: { b: 'hello', x: 'extra' }, id: test:2 }]"

[[test.results]]
value = "[{ id: test:1 }, { a: { b: 'hello', x: 'extra' }, id: test:2 }]"

*/

-- FLEXIBLE with option<object>
DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD a ON test TYPE option<object> FLEXIBLE;
DEFINE FIELD a.b ON test TYPE string;

CREATE test:1;
CREATE test:2 SET a = { b: 'hello', x: 'extra' }; -- x is allowed
SELECT * FROM test ORDER BY id;

