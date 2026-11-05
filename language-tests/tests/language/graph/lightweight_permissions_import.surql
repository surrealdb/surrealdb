/**
[test]
reason = "Setup: a lightweight relation whose SELECT permission is conditional on the synthesized edge's fields, and whose DELETE permission is closed — so the permission gates must work against records that exist only as adjacency keys."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;

-- Edges are visible only to their own `in` endpoint, and cannot be deleted
-- by record users at all.
DEFINE TABLE follows TYPE RELATION IN person OUT person LIGHTWEIGHT
	PERMISSIONS FOR select WHERE in = $auth FOR delete NONE;

CREATE person:alice, person:bob, person:carol;

RELATE person:bob->follows->person:alice;
RELATE person:carol->follows->person:bob;
