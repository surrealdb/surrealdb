/**
[test]
reason = "Setup for return_none_select_gate_effects.surql: tables whose select permission draws randomness or makes a denied external read, and views showing the stored rows."
run = false
*/

DEFINE TABLE user SCHEMALESS;
CREATE user:alice;
DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('user', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE drawn SCHEMALESS PERMISSIONS FOR select WHERE rand() < 2 FOR create, update, delete FULL;
DEFINE TABLE fetched SCHEMALESS
	PERMISSIONS FOR select WHERE http::get('http://127.0.0.1:1/') != NONE FOR create, update, delete FULL;
DEFINE TABLE effect_state AS SELECT id, n FROM drawn PERMISSIONS FULL;
DEFINE TABLE fetched_state AS SELECT id, n FROM fetched PERMISSIONS FULL;
CREATE drawn:1 SET n = 1;
CREATE fetched:1 SET n = 1;
