/**
[test]
reason = "Setup: an edge table whose records are gated by a conditional table-level SELECT permission, with the lowest-id edge non-public. A LIMIT pushed into the graph scan must count visible rows, not scanned candidates, so the hidden edge must never consume the cap."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;

-- Edge records visible only when flagged public.
DEFINE TABLE knows SCHEMALESS TYPE RELATION FROM person TO person
	PERMISSIONS FOR select WHERE public = true;

CREATE person:alice, person:bob, person:carol;

RELATE person:bob->knows:e1->person:alice SET public = false;
RELATE person:bob->knows:e2->person:carol SET public = true;
