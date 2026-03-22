/**
[test]
reason = "REMOVE FIELD with non-field idiom parts (array indices) must work correctly"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD tags ON test TYPE array<string>;
REMOVE FIELD tags[*] ON test;
REMOVE FIELD IF EXISTS tags[*] ON test;
