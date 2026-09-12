/**
[test]
reason = """
	Setup for layers_count_fast_path and its plan companion: a table
	with a computed field whose body reads another record, so that the body
	can be made to fail by a write the record user is allowed to make, and a
	table permission that admits the record user. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader;

DEFINE TABLE other SCHEMALESS PERMISSIONS FULL;
CREATE other:1 SET v = 1;

DEFINE TABLE t SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD a ON t TYPE int;
DEFINE FIELD n ON t TYPE int COMPUTED (SELECT VALUE v FROM ONLY other:1);
CREATE t:1 SET a = 1;
CREATE t:2 SET a = 2;
