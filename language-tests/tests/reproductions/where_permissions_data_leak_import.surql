/**
[test]
reason = "Import: setup tables with restricted permissions for WHERE clause data leak test"
run = false
*/

DEFINE TABLE accounts SCHEMALESS
	PERMISSIONS
		FOR select WHERE false
		FOR update WHERE false
		FOR delete WHERE false
		FOR create FULL;

CREATE accounts:1 SET secret = 'alpha';
CREATE accounts:2 SET secret = 'bravo';
CREATE accounts:3 SET secret = 'charlie';

DEFINE TABLE friends TYPE RELATION SCHEMALESS
	PERMISSIONS
		FOR select WHERE false
		FOR update WHERE false
		FOR delete WHERE false
		FOR create FULL;

RELATE accounts:1 -> friends:1 -> accounts:2 SET secret = 'edge_secret';
