/**
[test]
reason = """
	Setup for id_components_guard_derivations: a table whose create, update,
	delete and select permissions all read a component of its record id, a table
	a record user may write to but whose rows it may read only for its own city,
	an event on that table that records the components it saw, and the record
	user the test runs as. Runs as owner.
"""
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE TABLE witness SCHEMALESS PERMISSIONS FULL;
CREATE user:london SET site = 'London' RETURN NONE;

-- Every verb is judged by a predicate that reads a component.
DEFINE TABLE reading SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE city = $auth.site;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 4 RETURN NONE;
CREATE reading:['Paris', d'2024-01-01T00:00:00Z'] SET temp = 6 RETURN NONE;

-- A write is allowed; what it returns is judged by a predicate that reads a component.
DEFINE TABLE ledger SCHEMALESS PERMISSIONS FOR create, update, delete FULL, FOR select WHERE city = $auth.site;
DEFINE FIELD id ON ledger TYPE [city: string, time: datetime];
DEFINE EVENT seen ON ledger THEN (CREATE witness SET city = $after.city, n = $after.n);
