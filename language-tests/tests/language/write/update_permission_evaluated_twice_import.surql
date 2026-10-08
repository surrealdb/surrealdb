/**
[test]
reason = "Setup: an update permission whose predicate counts its own evaluations, on both a normal and a relation table, so every update-shaped statement can be asked how many times it judged the clause."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE counter SCHEMALESS PERMISSIONS FULL;

-- An update clause is a write clause, so its predicate may write; the bump is
-- the evaluation count, and the comparison is always true so the clause itself
-- never refuses.
DEFINE TABLE thing SCHEMALESS
	PERMISSIONS FOR select, create FULL,
	FOR update WHERE (UPSERT ONLY counter:1 SET n += 1).n >= 0;

DEFINE TABLE edge TYPE RELATION IN person OUT person SCHEMALESS
	PERMISSIONS FOR select, create FULL,
	FOR update WHERE (UPSERT ONLY counter:1 SET n += 1).n >= 0;

CREATE person:bob;
CREATE counter:1 SET n = 0;
CREATE thing:1 SET x = 1;
