/**
[test]
reason = "Setup for return_none_select_gate_inert.surql and return_none_explain.surql: a record access method, an `item` table and a `link` table whose select permissions read stored fields, and two materialised views that show the stored rows whatever those permissions say."
run = false
*/

DEFINE TABLE user SCHEMALESS;
CREATE user:alice, user:bob;
DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('user', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE item SCHEMALESS
	PERMISSIONS FOR select WHERE owner = $auth.id FOR create, update, delete FULL;
DEFINE TABLE link SCHEMALESS
	PERMISSIONS FOR select WHERE n < 100 FOR create, update, delete FULL;
DEFINE TABLE item_state AS SELECT id, owner, n FROM item PERMISSIONS FULL;
DEFINE TABLE link_state AS SELECT id, in, out, n FROM link PERMISSIONS FULL;

CREATE item:1 SET owner = user:alice, n = 1;
CREATE item:2 SET owner = user:bob, n = 2;
CREATE item:3 SET owner = user:alice, n = 3;
