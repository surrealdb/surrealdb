/**
[env]
auth = { level = "viewer" }

[test]
reason = "Viewer cannot execute other DDL: DEFINE SEQUENCE, DEFINE CONFIG"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

*/
DEFINE SEQUENCE test_seq;
DEFINE CONFIG GRAPHQL AUTO;
