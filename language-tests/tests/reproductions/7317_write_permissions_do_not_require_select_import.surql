/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Setup for 7317_write_permissions_do_not_require_select.surql: an audit table the writer may read, an event that records each write to `node`, and a table whose select permission denies everything and whose create, update and delete permissions admit everything."
run = false
*/

DEFINE TABLE node SCHEMAFULL
	PERMISSIONS
		FOR select WHERE false,
		FOR create WHERE true,
		FOR update WHERE true,
		FOR delete WHERE true;
DEFINE FIELD touched ON node TYPE option<bool>;
DEFINE TABLE audit SCHEMALESS PERMISSIONS FOR select FULL;
CREATE node:keep;
DEFINE EVENT log ON node THEN { CREATE audit SET what = $event; };
