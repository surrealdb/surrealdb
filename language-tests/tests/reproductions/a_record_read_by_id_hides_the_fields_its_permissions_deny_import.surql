/**
[test]
reason = "Setup: a table whose fields a record user may not read — top-level, nested, array element and computed — and records that reach it by link and by edge."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD top ON t PERMISSIONS FOR select WHERE false;
DEFINE FIELD obj ON t TYPE object PERMISSIONS FULL;
DEFINE FIELD obj.secret ON t PERMISSIONS FOR select WHERE false;
DEFINE FIELD items ON t TYPE array<object> PERMISSIONS FULL;
DEFINE FIELD items[*].secret ON t PERMISSIONS FOR select WHERE false;
DEFINE FIELD twice ON t COMPUTED shown * 2 PERMISSIONS FOR select WHERE false;
DEFINE TABLE holder SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE rel TYPE RELATION PERMISSIONS FULL;

CREATE person:bob;
CREATE t:1 SET shown = 1, top = 'TOP', obj = { open: 1, secret: 'OBJ' },
	items = [{ open: 1, secret: 'ITEM' }];
CREATE holder:1 SET link = t:1;
RELATE holder:1->rel->t:1;
