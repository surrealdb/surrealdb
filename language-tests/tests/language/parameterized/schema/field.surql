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
value = '''{ "test.nested": "DEFINE FIELD test.nested ON test COMMENT 'test comment' PERMISSIONS FULL" }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"

*/
LET $name = "test.nested";
LET $table = "test";
LET $comment = "test comment";

DEFINE FIELD $name ON $table COMMENT $comment;

(INFO FOR TABLE $table).fields;

REMOVE FIELD $name ON $table;

(INFO FOR TABLE $table).fields;