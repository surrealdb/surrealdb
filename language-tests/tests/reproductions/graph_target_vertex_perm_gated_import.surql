/**
[test]
reason = "Setup: a graph schema whose edge table has a per-row SELECT permission. Used to assert that the planner skips the `->edge->vertex` fast-path collapse and the WHERE continues to gate traversal."
run = false
*/

DEFINE TABLE person SCHEMALESS PERMISSIONS FOR select FULL;

-- Edge table with a `Specific` SELECT permission: this disqualifies it
-- from the planner's fast-path collapse so the per-row WHERE has to be
-- evaluated against the edge record before the target vertex is yielded.
DEFINE TABLE friend TYPE RELATION SCHEMALESS
	PERMISSIONS
		FOR select WHERE in.id = $auth.id
		FOR create FULL;

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

CREATE person:alice, person:bob, person:carol, person:dave;

-- Alice's edges -- visible to alice via the WHERE. Explicit edge IDs
-- so the iteration order is deterministic across backends.
RELATE person:alice -> friend:f1 -> person:bob;
RELATE person:alice -> friend:f2 -> person:carol;

-- Bob's edge -- hidden from alice via the WHERE.
RELATE person:bob -> friend:f3 -> person:dave;
