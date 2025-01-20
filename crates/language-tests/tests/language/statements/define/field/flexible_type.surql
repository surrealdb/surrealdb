/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found '1' for field `a`, with record `t:1`, but expected a number"

*/

DEFINE TABLE t;
DEFINE FIELD a ON t FLEXIBLE TYPE number;

CREATE t:1 SET a = "1";
