/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Setup for 7464_auth_id_when_the_session_cannot_read_its_record.surql: the table of the signed-in record permits nothing, the record exists, and a table stamps and gates on `$auth.id` in a VALUE, a COMPUTED and a permission clause."
run = false
*/

DEFINE TABLE person SCHEMALESS PERMISSIONS NONE;
CREATE person:bob SET name = 'bob';
DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select, create, delete WHERE owner = $auth.id;
DEFINE FIELD stamp ON doc VALUE $auth.id;
DEFINE FIELD stamp_name ON doc VALUE $auth.name;
DEFINE FIELD viewer ON doc COMPUTED $auth.id;
