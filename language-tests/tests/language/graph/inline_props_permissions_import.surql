/**
[test]
reason = "Setup: an edge table whose INLINE field is hidden from record users by a field-level SELECT permission. The payloads embed the raw values, so a record user's filtered traversal must be forced onto the record path, where the hidden field is nulled before the predicate sees it."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE likes SCHEMALESS TYPE RELATION PERMISSIONS FOR select FULL;
DEFINE FIELD score ON likes TYPE number INLINE PERMISSIONS FOR select NONE;

CREATE person:alice, person:bob;
RELATE person:bob->likes:e1->person:alice SET score = 9;
RELATE person:bob->likes:e2->person:alice SET score = 1;
