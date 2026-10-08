/**
[test]
run = false
*/

-- Fixture for `perm_clause_holding_a_write.gql`: a table whose SELECT
-- permission is a conditional whose untaken branch holds a write.
--
-- A stored clause is accepted with a write in it, and the condition is
-- evaluated without ever running the branch. The clause has a logical form, so
-- it is compiled — and no write operator exists yet, so compiling it declines
-- the whole plan. A SurrealQL statement would fall back; a GQL plan has no
-- statement behind it and no fallback, so it would fail outright on a read
-- that never needed the branch.
DEFINE TABLE person SCHEMAFULL
	PERMISSIONS FOR select WHERE IF false THEN (CREATE audit:1).id != NONE ELSE true END;

DEFINE FIELD name ON person TYPE string PERMISSIONS FULL;

DEFINE TABLE knows TYPE RELATION SCHEMALESS PERMISSIONS FOR select FULL;

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('person', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

CREATE person:1 SET name = 'alice' RETURN NONE;
CREATE person:2 SET name = 'bob' RETURN NONE;

INSERT RELATION INTO knows [
	{ id: knows:k12, in: person:1, out: person:2 }
] RETURN NONE;
