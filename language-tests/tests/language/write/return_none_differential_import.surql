/**
[test]
reason = "Setup for return_none_differential.surql: twin tables `a` and `b` with a changefeed, an event that appends to an audit table, a computed field and a select permission that reads it, and views showing the stored rows and the audit rows."
run = false
*/

DEFINE TABLE user SCHEMALESS;
CREATE user:alice;
DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('user', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE a SCHEMALESS CHANGEFEED 1d
	PERMISSIONS FOR select WHERE d >= 0 AND n < 1000 FOR create, update, delete FULL;
DEFINE FIELD d ON a COMPUTED n * 2;
DEFINE TABLE b SCHEMALESS CHANGEFEED 1d
	PERMISSIONS FOR select WHERE d >= 0 AND n < 1000 FOR create, update, delete FULL;
DEFINE FIELD d ON b COMPUTED n * 2;
DEFINE TABLE audit SCHEMALESS PERMISSIONS FULL;
DEFINE EVENT ev ON a THEN (CREATE audit SET src = 'a', event = $event, rid = record::id($after.id ?? $before.id), n = $after.n ?? $before.n);
DEFINE EVENT ev ON b THEN (CREATE audit SET src = 'b', event = $event, rid = record::id($after.id ?? $before.id), n = $after.n ?? $before.n);
DEFINE TABLE state_a AS SELECT id, n, tag FROM a PERMISSIONS FULL;
DEFINE TABLE state_b AS SELECT id, n, tag FROM b PERMISSIONS FULL;
CREATE a:9 SET n = 9, tag = 'seed';
CREATE b:9 SET n = 9, tag = 'seed';
