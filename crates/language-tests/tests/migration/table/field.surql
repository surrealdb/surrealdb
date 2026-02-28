/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/

DEFINE TABLE t;
DEFINE FIELD f.* ON t;

migration::diagnose()
