/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Setup for 7464_auth_id_when_the_id_field_is_hidden.surql: the signed-in record's table is readable, but its `id` field is not."
run = false
*/

DEFINE TABLE person SCHEMALESS PERMISSIONS FOR select WHERE id = $auth;
DEFINE FIELD id ON person PERMISSIONS FOR select NONE;
CREATE person:bob SET name = 'bob';
