/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'example' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'example' does not exist"

[[test.results]]
value = "NONE"

*/

DEFINE TABLE example;
DEFINE TABLE IF NOT EXISTS example;
DEFINE TABLE OVERWRITE example;
DEFINE TABLE example;
REMOVE TABLE IF EXISTS example;
REMOVE TABLE example;
REMOVE TABLE IF EXISTS example;
