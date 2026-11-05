/**
[test]
reason = """
	Setup for layers_name_collision: a table whose field names differ
	only by a trailing ordinal — `a1` with nine other computed fields beside
	`a`, and `_a1` beside `_a10` — one of each pair denied to the record
	user. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader;

DEFINE TABLE u SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD a1 ON u TYPE int COMPUTED 1;
DEFINE FIELD b ON u TYPE int COMPUTED 2;
DEFINE FIELD c ON u TYPE int COMPUTED 3;
DEFINE FIELD d ON u TYPE int COMPUTED 4;
DEFINE FIELD e ON u TYPE int COMPUTED 5;
DEFINE FIELD f ON u TYPE int COMPUTED 6;
DEFINE FIELD g ON u TYPE int COMPUTED 7;
DEFINE FIELD h ON u TYPE int COMPUTED 8;
DEFINE FIELD i ON u TYPE int COMPUTED 9;
DEFINE FIELD j ON u TYPE int COMPUTED 10;
DEFINE FIELD a ON u TYPE string PERMISSIONS FOR select NONE;
DEFINE FIELD _a1 ON u TYPE int COMPUTED 11;
DEFINE FIELD _a10 ON u TYPE string PERMISSIONS FOR select NONE;
CREATE u:1 SET a = 'secret-a', _a10 = 'secret-a10';
