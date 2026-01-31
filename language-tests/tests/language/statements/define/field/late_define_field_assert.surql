/**
[test]

[[test.results]]
value = "[{ a: 1, b: '2', id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found 1 for field `a`, with record `t:2`, but field must conform to: $value != 1"

[[test.results]]
value = "[{ a: 1, b: '2', id: t:1 }]"

*/

CREATE t:1 SET a = 1, b = "2";

DEFINE FIELD a ON TABLE t ASSERT $value != 1;

CREATE t:2 SET a = 1;

SELECT * FROM t;
