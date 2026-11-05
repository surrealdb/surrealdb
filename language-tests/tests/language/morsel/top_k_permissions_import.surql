/**
[test]
reason = "Setup: a record access method and a SCHEMAFULL table whose rows a record user sees in part, with a field it may not select."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
CREATE person:bob;

DEFINE TABLE item SCHEMAFULL PERMISSIONS FOR select WHERE n % 3 != 0;
DEFINE FIELD n ON item TYPE int;
DEFINE FIELD k ON item TYPE int;
DEFINE FIELD hidden ON item TYPE int PERMISSIONS FOR select NONE;
FOR $i IN 0..1500 {
	CREATE type::record('item', $i) SET n = $i, k = $i % 11, hidden = 1500 - $i RETURN NONE
};
