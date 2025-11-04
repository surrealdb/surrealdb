/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The namespace 'example' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The namespace 'example' does not exist"

[[test.results]]
value = "NONE"

*/

DEFINE NAMESPACE example;
DEFINE NAMESPACE IF NOT EXISTS example;
DEFINE NAMESPACE OVERWRITE example;
DEFINE NAMESPACE example;
REMOVE NAMESPACE IF EXISTS example;
REMOVE NAMESPACE example;
REMOVE NAMESPACE IF EXISTS example;
