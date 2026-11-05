/**
[test]
reason = "Setup for the test that an event's THEN clauses read tables the record user who fired it cannot read."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid );

DEFINE TABLE user SCHEMALESS PERMISSIONS NONE;
DEFINE TABLE secret SCHEMALESS PERMISSIONS NONE;
DEFINE TABLE src SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE log SCHEMALESS PERMISSIONS FOR select FULL;

CREATE user:a;
CREATE secret:1 SET v = 1;
CREATE secret:2 SET v = 2;

DEFINE EVENT block_body ON src WHEN $event = 'CREATE' THEN {
	CREATE log:block SET seen = (SELECT VALUE v FROM secret ORDER BY v)
};
DEFINE EVENT expr_body ON src WHEN $event = 'CREATE'
	THEN (CREATE log:expr SET seen = (SELECT VALUE v FROM secret ORDER BY v));
DEFINE EVENT when_reads ON src WHEN count(SELECT * FROM secret) = 2
	THEN (CREATE log:when SET seen = true);
