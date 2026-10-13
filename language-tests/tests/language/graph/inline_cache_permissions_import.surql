/**
[test]
reason = "Setup: vertex and referenced tables carrying inline cache caps, with edge and referencing tables whose records are gated by conditional table-level SELECT permissions. The caches are materialised by the owner's writes and hold every entry — including the ones a record user must not see — so the cache-served read path has to enforce the same permission filtering as the scan it replaces."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS INLINE EDGES 8 INLINE REFERENCES 8 PERMISSIONS FULL;

-- Edge records visible only when flagged public.
DEFINE TABLE knows SCHEMALESS TYPE RELATION FROM person TO person
	PERMISSIONS FOR select WHERE public = true;

-- Referencing records visible only when flagged public.
DEFINE TABLE comment SCHEMALESS PERMISSIONS FOR select WHERE public = true;
DEFINE FIELD author ON comment TYPE record<person> REFERENCE;

CREATE person:alice, person:bob, person:carol;

RELATE person:bob->knows:e1->person:alice SET public = true;
RELATE person:bob->knows:e2->person:carol SET public = false;

CREATE comment:c1 SET author = person:bob, public = true;
CREATE comment:c2 SET author = person:bob, public = false;
