/**
[test]
reason = """
	Setup for layers_udf_permission_frame: a membership table whose
	`active` field denies the record user and whose `tier` is computed from
	`active` and denied outright, a user-defined function reading `tier`, and
	a document table whose `select` guard calls that function. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader;

DEFINE TABLE membership SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD who ON membership TYPE record<user>;
DEFINE FIELD active ON membership TYPE bool PERMISSIONS FOR select WHERE false;
DEFINE FIELD tier ON membership TYPE string
	COMPUTED IF active THEN 'gold' ELSE 'none' END
	PERMISSIONS FOR select NONE;
DEFINE FUNCTION fn::tier_of($u: record<user>) {
	RETURN (SELECT VALUE tier FROM ONLY membership WHERE who = $u LIMIT 1) ?? 'none';
} PERMISSIONS FULL;
DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE fn::tier_of($auth.id) = 'gold';

CREATE membership:1 SET who = user:reader, active = true;
CREATE doc:1 SET title = 'd';
