/**
[env]
auth = { level = "viewer" }

[test]
reason = "Viewer cannot execute access/user DDL: DEFINE ACCESS, DEFINE USER"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

*/
DEFINE ACCESS test_ac ON DATABASE TYPE BEARER FOR USER;
DEFINE USER test_usr ON DATABASE PASSWORD 'secret' ROLES VIEWER;
