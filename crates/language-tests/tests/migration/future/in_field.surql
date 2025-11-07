/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/

# Can be migrated without issue;
DEFINE FIELD f ON t1 VALUE <future> { 1 };
DEFINE FIELD f ON t2 VALUE ((<future> { 1 }));
DEFINE FIELD f ON t3 DEFAULT <future> { 1 };

migration::diagnose()
