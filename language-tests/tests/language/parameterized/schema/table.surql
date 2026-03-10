/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ test_table: "DEFINE TABLE test_table TYPE ANY SCHEMALESS COMMENT 'test comment' PERMISSIONS NONE" }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"

*/
LET $name = "test_table";
LET $comment = "test comment";

DEFINE TABLE $name COMMENT $comment;

(INFO FOR DB).tables;

REMOVE TABLE $name;

(INFO FOR DB).tables;
