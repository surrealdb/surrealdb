/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The index 'example' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The index 'example' does not exist"

[[test.results]]
value = "NONE"

*/

DEFINE INDEX example ON example FIELDS example;
DEFINE INDEX IF NOT EXISTS example ON example FIELDS example;
DEFINE INDEX OVERWRITE example ON example FIELDS example;
DEFINE INDEX example ON example FIELDS example;
REMOVE INDEX IF EXISTS example ON example;
REMOVE INDEX example ON example;
REMOVE INDEX IF EXISTS example ON example;
