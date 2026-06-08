/**
[test]
reason = "Setup: a graph schema where the edge table denies SELECT. Used to assert that a record user cannot traverse the edge to enumerate its rows."
run = false
*/

DEFINE TABLE node SCHEMALESS
	PERMISSIONS
		FOR select FULL
		FOR create FULL;

-- Edge table whose SELECT permission is NONE: a record user must NOT be
-- able to traverse `->edge` and enumerate it.
DEFINE TABLE edge TYPE RELATION SCHEMALESS
	PERMISSIONS
		FOR select NONE
		FOR create FULL;

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid );

CREATE node:a, node:b, node:c;
RELATE node:a -> edge -> node:b SET secret = 'first';
RELATE node:a -> edge -> node:c SET secret = 'second';
