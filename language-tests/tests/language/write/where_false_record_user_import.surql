/**
[test]
reason = "Setup for where_false_record_user.surql: a table whose update and delete permissions raise on one record, a table whose update and delete permissions refuse every record, and a table that permits every write."
run = false
*/

DEFINE TABLE user SCHEMALESS;
CREATE user:alice;
DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('user', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE g SCHEMALESS
	PERMISSIONS FOR select, create FULL FOR update, delete WHERE <int> code > 0;
CREATE g:1 SET code = 1;
CREATE g:2 SET code = 'x';
CREATE g:3 SET code = 3;

DEFINE TABLE denied SCHEMALESS PERMISSIONS FOR select FULL FOR create, update, delete NONE;
CREATE denied:1 SET code = 1;

DEFINE TABLE open SCHEMALESS PERMISSIONS FULL;
CREATE open:1 SET code = 1;
