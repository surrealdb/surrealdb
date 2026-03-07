/**
[env]
auth = { level = "editor" }

[test]
reason = "Editor cannot DEFINE/REMOVE: Access, User, Config, Sequence, or ALTER SEQUENCE"

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
DEFINE ACCESS editor_ac ON DATABASE TYPE BEARER FOR USER;
DEFINE USER editor_usr ON DATABASE PASSWORD 'secret' ROLES VIEWER;
DEFINE CONFIG GRAPHQL AUTO;
DEFINE SEQUENCE editor_seq;
REMOVE ACCESS editor_ac ON DATABASE;
REMOVE USER editor_usr ON DATABASE;
ALTER SEQUENCE editor_seq;
