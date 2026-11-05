/**
[test]
reason = "Setup for return_none_error_order.surql: three records, the second of which a select permission raises on and the third of which fails an ASSERT once its number is rewritten."
run = false
*/

DEFINE TABLE user SCHEMALESS;
CREATE user:alice;
DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('user', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE ordered SCHEMAFULL
	PERMISSIONS FOR select WHERE <int> code > 0 FOR create, update, delete FULL;
DEFINE FIELD code ON ordered TYPE any;
DEFINE FIELD n ON ordered TYPE int ASSERT $value != 99;
DEFINE TABLE ordered_state AS SELECT id, n FROM ordered PERMISSIONS FULL;
CREATE ordered:1 SET code = 1, n = 1;
CREATE ordered:2 SET code = 'x', n = 2;
CREATE ordered:3 SET code = 3, n = 3;
