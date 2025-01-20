/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found 1 for field `a`, with record `t:1`, but field must conform to: $value != 1"

*/

DEFINE TABLE t;
DEFINE FIELD a ON t FLEXIBLE ASSERT $value != 1;

CREATE t:1 SET a = 1;
