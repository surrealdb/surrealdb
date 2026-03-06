/**
[env]
auth = { level = "viewer" }

[test]
reason = "Viewer cannot execute ALTER statements"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

*/
ALTER TABLE test_tbl COMMENT 'changed';
ALTER SEQUENCE test_seq;
