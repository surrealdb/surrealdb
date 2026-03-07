/**
[env]
auth = { level = "viewer" }

[test]
reason = "Viewer cannot execute REMOVE statements"

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

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

*/
REMOVE TABLE test_tbl;
REMOVE FIELD name ON test_tbl;
REMOVE INDEX idx ON test_tbl;
REMOVE EVENT ev ON test_tbl;
REMOVE PARAM $test;
REMOVE FUNCTION fn::test;
REMOVE ANALYZER test_az;
REMOVE ACCESS test_ac ON DATABASE;
REMOVE USER test_usr ON DATABASE;
