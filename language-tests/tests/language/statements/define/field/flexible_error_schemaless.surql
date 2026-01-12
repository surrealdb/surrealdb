/**
[test]
reason = "Test that FLEXIBLE in schemaless table throws an error"

[[test.results]]
value = "NONE"

[[test.results]]
error = "An error occurred: FLEXIBLE can only be used in SCHEMAFULL tables"

*/

-- FLEXIBLE in schemaless table should error
DEFINE TABLE test; -- schemaless table
DEFINE FIELD a ON test TYPE object FLEXIBLE;

