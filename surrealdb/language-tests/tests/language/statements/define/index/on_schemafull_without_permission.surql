/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "The field 'foo' does not exist"

*/
DEFINE TABLE test SCHEMAFULL PERMISSIONS NONE;
DEFINE INDEX idx ON test FIELDS foo;
