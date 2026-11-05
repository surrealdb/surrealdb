/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ test: 'DEFINE FIELD test ON test TYPE object PERMISSIONS FULL', "test.nested": "DEFINE FIELD test.nested ON test COMMENT 'test comment' PERMISSIONS FULL" }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ test: 'DEFINE FIELD test ON test TYPE object PERMISSIONS FULL' }"

*/
LET $name = "test.nested";
LET $table = "test";
LET $comment = "test comment";

-- A nested field is defined in a field that holds it.
DEFINE FIELD test ON $table TYPE object;
DEFINE FIELD $name ON $table COMMENT $comment;

(INFO FOR TABLE $table).fields;

REMOVE FIELD $name ON $table;

(INFO FOR TABLE $table).fields;
