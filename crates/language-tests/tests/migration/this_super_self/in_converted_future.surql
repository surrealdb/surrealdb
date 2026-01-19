/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/

DEFINE FIELD f ON t1 VALUE <future> { $this };
migration::diagnose()
