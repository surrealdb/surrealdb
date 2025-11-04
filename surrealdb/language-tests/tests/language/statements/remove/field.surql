/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The field 'example' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The field 'example' does not exist"

[[test.results]]
value = "NONE"

*/

DEFINE FIELD example ON example;
DEFINE FIELD IF NOT EXISTS example ON example;
DEFINE FIELD OVERWRITE example ON example;
DEFINE FIELD example ON example;
REMOVE FIELD IF EXISTS example ON example;
REMOVE FIELD example ON example;
REMOVE FIELD IF EXISTS example ON example;
