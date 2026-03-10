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
value = """{ status: "DEFINE FIELD status ON test_table DEFAULT $default_status ASSERT $value INSIDE $allowed_statuses COMMENT 'test comment' PERMISSIONS FULL" }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ count: "DEFINE FIELD count ON test_table TYPE int DEFAULT $default_count VALUE $value OR $default_count COMMENT 'test comment' PERMISSIONS FULL", status: "DEFINE FIELD status ON test_table DEFAULT $default_status ASSERT $value INSIDE $allowed_statuses COMMENT 'test comment' PERMISSIONS FULL" }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ computed: "DEFINE FIELD computed ON test_table VALUE $default_count + 1 COMMENT 'test comment' PERMISSIONS FULL", count: "DEFINE FIELD count ON test_table TYPE int DEFAULT $default_count VALUE $value OR $default_count COMMENT 'test comment' PERMISSIONS FULL", status: "DEFINE FIELD status ON test_table DEFAULT $default_status ASSERT $value INSIDE $allowed_statuses COMMENT 'test comment' PERMISSIONS FULL" }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"

[[test.results]]
value = "NONE"


*/
LET $table = "test_table";
LET $field1 = "status";
LET $field2 = "count";
LET $field3 = "computed";
LET $default_status = "pending";
LET $allowed_statuses = ["pending", "active", "completed"];
LET $default_count = 0;
LET $comment = "test comment";

-- Create table first
DEFINE TABLE $table;

-- Test DEFINE FIELD with DEFAULT clause using parameter
DEFINE FIELD $field1 ON $table DEFAULT $default_status ASSERT $value IN $allowed_statuses COMMENT $comment;

(INFO FOR TABLE $table).fields;

-- Test DEFINE FIELD with DEFAULT and VALUE clauses using parameters
DEFINE FIELD $field2 ON $table TYPE int DEFAULT $default_count VALUE $value OR $default_count COMMENT $comment;

(INFO FOR TABLE $table).fields;

-- Test DEFINE FIELD with VALUE clause using parameter (computed field)
DEFINE FIELD $field3 ON $table VALUE $default_count + 1 COMMENT $comment;

(INFO FOR TABLE $table).fields;

-- Cleanup
REMOVE FIELD $field1 ON $table;
REMOVE FIELD $field2 ON $table;
REMOVE FIELD $field3 ON $table;

(INFO FOR TABLE $table).fields;

REMOVE TABLE $table;
