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
value = """{ test_table: "DEFINE TABLE test_table TYPE ANY SCHEMALESS COMMENT 'test comment' PERMISSIONS FOR select WHERE role = $user_role, FOR create, update, delete NONE" }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ test_field: "DEFINE FIELD test_field ON test_table COMMENT 'test comment' PERMISSIONS FOR select WHERE role = $user_role, FOR create, update FULL" }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"


*/
LET $table_name = "test_table";
LET $field_name = "test_field";
LET $user_role = "admin";
LET $comment = "test comment";

-- Test DEFINE TABLE with PERMISSIONS WHERE using parameter
DEFINE TABLE $table_name PERMISSIONS FOR SELECT WHERE role = $user_role COMMENT $comment;

(INFO FOR DB).tables;

-- Test DEFINE FIELD with PERMISSIONS WHERE using parameter
DEFINE FIELD $field_name ON $table_name PERMISSIONS FOR SELECT WHERE role = $user_role COMMENT $comment;

(INFO FOR TABLE $table_name).fields;

-- Cleanup
REMOVE FIELD $field_name ON $table_name;

(INFO FOR TABLE $table_name).fields;

REMOVE TABLE $table_name;

(INFO FOR DB).tables;
