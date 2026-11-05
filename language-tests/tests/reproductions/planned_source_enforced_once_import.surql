/**
[test]
reason = """
	Setup for planned_source_enforced_once and its plan companion: a
	table whose own select permission admits only the records with
	`secret = 'x'`, a field guard admitting `hidden` only to the record's
	owner, and the record user the tests run as. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader;
CREATE user:other;

DEFINE TABLE t SCHEMAFULL PERMISSIONS FOR select WHERE secret = 'x';
DEFINE FIELD secret ON t TYPE string;
DEFINE FIELD owner ON t TYPE record<user>;
DEFINE FIELD hidden ON t TYPE string PERMISSIONS FOR select WHERE owner = $auth.id;
DEFINE FIELD n ON t TYPE int;
CREATE t:1 SET secret = 'x', owner = user:reader, hidden = 'mine', n = 1;
CREATE t:2 SET secret = 'x', owner = user:other, hidden = 'theirs', n = 2;
CREATE t:3 SET secret = 'y', owner = user:reader, hidden = 'gone', n = 3;
