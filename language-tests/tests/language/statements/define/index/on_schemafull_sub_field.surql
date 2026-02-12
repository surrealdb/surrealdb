/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "The field 'document.visible' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The field 'document.visible' does not exist"

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
DEFINE INDEX idx ON test FIELDS document.visible;

DEFINE FIELD document.visible ON test TYPE bool;
DEFINE INDEX idx ON test FIELDS document.visible;

REMOVE FIELD document.visible ON test;
DEFINE FIELD document ON test TYPE bool;
DEFINE INDEX OVERWRITE idx ON test FIELDS document.visible;

DEFINE FIELD OVERWRITE document ON test TYPE object;
DEFINE INDEX OVERWRITE idx ON test FIELDS document.visible;

DEFINE FIELD OVERWRITE document ON test TYPE { visible: bool };
DEFINE INDEX OVERWRITE idx ON test FIELDS document.visible;
