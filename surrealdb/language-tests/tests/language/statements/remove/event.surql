/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The event 'example' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The event 'example' does not exist"

[[test.results]]
value = "NONE"

*/

DEFINE EVENT example ON example THEN {};
DEFINE EVENT IF NOT EXISTS example ON example THEN {};
DEFINE EVENT OVERWRITE example ON example THEN {};
DEFINE EVENT example ON example THEN {};
REMOVE EVENT IF EXISTS example ON example;
REMOVE EVENT example ON example;
REMOVE EVENT IF EXISTS example ON example;
