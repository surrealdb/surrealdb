/**
[test]
reason = """
	Setup for subquery_rows_keep_hidden_fields: a record user `user:bob`; a
	`member` table whose `notes` field no caller may select, with an event that
	records `notes` before and after each update; and a `person` table each
	user may update only their own record of, reached through a `post` table
	anyone may read. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:bob;
CREATE user:alice;

DEFINE TABLE member SCHEMAFULL PERMISSIONS FOR select, update WHERE owner = $auth.id;
DEFINE FIELD owner ON member TYPE record<user>;
DEFINE FIELD name ON member TYPE string;
DEFINE FIELD age ON member TYPE option<int>;
DEFINE FIELD notes ON member TYPE option<string> PERMISSIONS FOR select NONE;
DEFINE TABLE audit SCHEMALESS PERMISSIONS FULL;
DEFINE EVENT noted ON member WHEN $event = 'UPDATE' THEN {
	CREATE audit SET member = $after.id, before = $before.notes, after = $after.notes;
};
CREATE member:1 SET owner = user:bob, name = 'bob', notes = 'hidden';
CREATE member:2 SET owner = user:alice, name = 'alice', notes = 'private';

DEFINE TABLE person SCHEMAFULL PERMISSIONS FOR select FULL, FOR update WHERE owner = $auth.id;
DEFINE FIELD owner ON person TYPE record<user>;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD age ON person TYPE option<int>;
CREATE person:alice SET owner = user:alice, name = 'alice';
CREATE person:bob SET owner = user:bob, name = 'bob';

DEFINE TABLE post SCHEMALESS PERMISSIONS FULL;
CREATE post:1 SET author = person:alice;
CREATE post:2 SET author = person:bob;
