/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Setup for 7464_auth_id_when_the_session_cannot_read_its_record.surql: the table of the signed-in record permits nothing, and the record exists."
run = false
*/

DEFINE TABLE person SCHEMALESS PERMISSIONS NONE;
CREATE person:bob SET name = 'bob';
