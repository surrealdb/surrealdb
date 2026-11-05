/**
[test]
reason = "Setup for the test that a bucket's PERMISSIONS clause reads a table the record user cannot read."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid );

DEFINE TABLE user SCHEMALESS PERMISSIONS NONE;
DEFINE TABLE grant SCHEMALESS PERMISSIONS NONE;

CREATE user:allowed, user:denied;
CREATE grant:1 SET who = user:allowed;

DEFINE BUCKET granted BACKEND "memory"
	PERMISSIONS WHERE $auth.id IN (SELECT VALUE who FROM grant);
f"granted:/file.txt".put("GRANTED");
