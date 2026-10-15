/**
[test]
reason = "Setup for return_images_move.surql and return_images_move_as_record_user.surql: a table with no field view, a table with a computed field, a table with a field the select permission cuts, and a record access method."
run = false
*/

DEFINE TABLE user SCHEMALESS;
CREATE user:alice;
DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('user', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE plain SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE comp SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD double ON comp COMPUTED n * 2;
DEFINE TABLE cut SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD secret ON cut PERMISSIONS FOR select NONE;
