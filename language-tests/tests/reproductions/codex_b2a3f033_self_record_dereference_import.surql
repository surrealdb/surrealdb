/**
[test]
reason = "Import: secrets table with a SELECT NONE field; record user auth"
run = false
*/

DEFINE TABLE secrets SCHEMAFULL
	PERMISSIONS
		FOR select FULL
		FOR create FULL
		FOR update, delete NONE;

DEFINE FIELD secret ON secrets TYPE string PERMISSIONS FOR select NONE;
DEFINE FIELD public ON secrets TYPE string;

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid );

CREATE secrets:test SET secret = 'hidden_value', public = 'visible';
