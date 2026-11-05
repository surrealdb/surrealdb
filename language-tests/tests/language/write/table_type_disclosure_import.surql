/**
[test]
reason = "Setup: relation and normal tables that deny creates to record users, one of them allowing updates, so the create gate and the table-type check can be told apart in the answer a denied actor gets."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;

-- Creates are closed to record users on every table below, so no write in the
-- test reaches a table's own create rule; what differs is only which check
-- speaks first, and whether anything downstream swallows what it said.
DEFINE TABLE edge TYPE RELATION IN person OUT person
	PERMISSIONS FOR create NONE, FOR select FULL;

-- Same, but updates are open, so UPSERT's fallback update cannot swallow the
-- error its create attempt raised.
DEFINE TABLE edge_updatable TYPE RELATION IN person OUT person
	PERMISSIONS FOR create NONE, FOR select FULL, FOR update FULL;

DEFINE TABLE plain TYPE NORMAL
	PERMISSIONS FOR create NONE, FOR select FULL;

CREATE person:bob;
