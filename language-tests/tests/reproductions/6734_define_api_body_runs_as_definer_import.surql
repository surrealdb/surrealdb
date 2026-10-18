/**
[test]
reason = "Setup for 6734_define_api_body_runs_as_definer.surql: a table with PERMISSIONS NONE, an API endpoint whose handler reads from it, and a record access method."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid );

DEFINE CONFIG API PERMISSIONS FULL;

DEFINE TABLE user SCHEMALESS PERMISSIONS NONE;
DEFINE TABLE secret SCHEMALESS PERMISSIONS NONE;

CREATE user:bob;
CREATE secret:1 SET v = 42;

DEFINE API "/secret"
	FOR get
		PERMISSIONS FULL
		THEN { RETURN { status: 200, body: (SELECT VALUE v FROM secret) } };
