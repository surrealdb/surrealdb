/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `a` of `t:1`: Expected `number` but found `'1'`"

*/

DEFINE TABLE t;
DEFINE FIELD a ON t FLEXIBLE TYPE number;

CREATE t:1 SET a = "1";
