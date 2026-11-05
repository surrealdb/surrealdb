/**
[test]
reason = "Setup: two tables a record user may delete from but not read — one whose select clause announces that it ran, one that denies outright and records the delete through an event instead."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE witness SCHEMALESS PERMISSIONS FULL;

DEFINE TABLE loud SCHEMALESS
	PERMISSIONS FOR create, delete FULL,
	FOR select WHERE IF true THEN THROW 'select gate ran' ELSE true END;

DEFINE TABLE quiet SCHEMALESS PERMISSIONS FOR create, delete FULL, FOR select NONE;

-- Events run before the output gate, so the witness records a delete the
-- statement itself is not allowed to report.
DEFINE EVENT witness ON quiet WHEN $event = 'DELETE' THEN (CREATE witness SET who = $before.id);

CREATE person:bob;
CREATE loud:1 SET x = 1;
CREATE quiet:1 SET x = 1;
