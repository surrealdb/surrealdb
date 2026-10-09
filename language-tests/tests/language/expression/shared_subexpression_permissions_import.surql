/**
[test]
reason = "Setup for shared_subexpression_permissions: a record access, a table whose field `a` is readable only while positive, a computed field that draws a value and a parameter no record user may read."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
CREATE person:bob;

DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD a ON t PERMISSIONS FOR select WHERE a > 0;
DEFINE FIELD c ON t COMPUTED rand::int(1, 1000000000);
DEFINE PARAM $q VALUE 3 PERMISSIONS NONE;
CREATE t:1 SET a = 3;
CREATE t:2 SET a = -1;
CREATE t:3 SET a = 5;
