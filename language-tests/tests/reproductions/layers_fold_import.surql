/**
[test]
reason = """
	Setup for layers_fold_pushdown: a table with two computed fields,
	a guarded field, an indexed stored field and a plain stored field, and
	the record user the plans are made for. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader;

DEFINE TABLE t SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD st ON t TYPE int;
DEFINE FIELD cat ON t TYPE string;
DEFINE FIELD name ON t TYPE string;
DEFINE FIELD len ON t TYPE int COMPUTED string::len(name);
DEFINE FIELD dbl ON t TYPE int COMPUTED string::len(name) * 2;
DEFINE FIELD secret ON t TYPE string PERMISSIONS FOR select WHERE $auth.id = user:reader;
DEFINE INDEX idx_cat ON t FIELDS cat;
CREATE t:1 SET st = 200, cat = 'a', name = 'abc', secret = 's1';
CREATE t:2 SET st = 404, cat = 'b', name = 'abcd', secret = 's2';
