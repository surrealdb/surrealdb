/**
[test]
reason = """
	Setup for layers_limit_pushdown and its plan companion: a table
	with a computed field over several rows, a table whose own select
	permission admits only the record's owner, and the record user the tests
	run as. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader;
CREATE user:other;

DEFINE TABLE t SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD n ON t TYPE int;
DEFINE FIELD dbl ON t TYPE int COMPUTED n * 2;
CREATE t:1 SET n = 1;
CREATE t:2 SET n = 2;
CREATE t:3 SET n = 3;
CREATE t:4 SET n = 4;
CREATE t:5 SET n = 5;

DEFINE TABLE gated SCHEMAFULL PERMISSIONS FOR select WHERE owner = $auth.id;
DEFINE FIELD owner ON gated TYPE record<user>;
CREATE gated:1 SET owner = user:other;
CREATE gated:2 SET owner = user:reader;
CREATE gated:3 SET owner = user:reader;
