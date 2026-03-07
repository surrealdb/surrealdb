/**
[env]
auth = { level = "viewer" }

[test]
reason = "Viewer cannot execute table-related DDL: DEFINE TABLE, FIELD, INDEX, EVENT, PARAM, FUNCTION, ANALYZER"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

*/
DEFINE TABLE test_tbl;
DEFINE FIELD name ON test_tbl TYPE string;
DEFINE INDEX idx ON test_tbl FIELDS name;
DEFINE EVENT ev ON test_tbl WHEN true THEN {};
DEFINE PARAM $test VALUE 1;
DEFINE FUNCTION fn::test() { RETURN 1; };
DEFINE ANALYZER test_az TOKENIZERS blank;
