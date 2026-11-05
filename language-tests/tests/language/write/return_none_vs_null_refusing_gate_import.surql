/**
[test]
reason = "Setup for return_none_vs_null_refusing_gate.surql: a table a record user may write but never select, one it may write and select, and a view showing the stored rows of the first."
run = false
*/

DEFINE TABLE user SCHEMALESS;
CREATE user:alice;
DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('user', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE sealed SCHEMALESS PERMISSIONS FOR select NONE FOR create, update, delete FULL;
DEFINE TABLE sealed_state AS SELECT id, n FROM sealed PERMISSIONS FULL;
CREATE sealed:1 SET n = 1;
CREATE sealed:2 SET n = 2;

DEFINE TABLE open_tb SCHEMALESS PERMISSIONS FULL;
CREATE open_tb:1 SET n = 1;
CREATE open_tb:2 SET n = 2;
