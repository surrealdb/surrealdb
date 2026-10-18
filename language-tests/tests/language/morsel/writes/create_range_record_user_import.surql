/**
[test]
reason = "Setup: a record access method and three tables a record user creates into under a full, a row-level and no create permission."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
CREATE person:bob;

DEFINE TABLE open SCHEMALESS PERMISSIONS FOR select FULL, FOR create FULL;
DEFINE FIELD n ON open VALUE record::id(id) * 2;

DEFINE TABLE some SCHEMALESS PERMISSIONS FOR select FULL, FOR create WHERE record::id(id) > 1200;
DEFINE FIELD n ON some VALUE record::id(id) * 2;

DEFINE TABLE shut SCHEMALESS PERMISSIONS FOR select FULL, FOR create NONE;
DEFINE FIELD n ON shut VALUE record::id(id) * 2;
