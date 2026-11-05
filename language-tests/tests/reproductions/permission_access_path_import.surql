/**
[test]
reason = """
	Setup for permission_access_path_redaction and its owner sibling:
	a table whose select permission admits one value of an indexed field, so
	that the permission predicate is what drives the access path, and the
	record user the tests run as. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader;

DEFINE TABLE t SCHEMAFULL PERMISSIONS FOR select WHERE tier = 'gold';
DEFINE FIELD tier ON t TYPE string;
DEFINE FIELD n ON t TYPE int;
DEFINE INDEX idx_tier ON t FIELDS tier;
CREATE t:1 SET tier = 'gold', n = 1;
CREATE t:2 SET tier = 'bronze', n = 2;
