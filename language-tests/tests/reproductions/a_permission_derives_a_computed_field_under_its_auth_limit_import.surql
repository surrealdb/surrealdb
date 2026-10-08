/**
[env]
namespace = true
database = true
auth = { namespace = "test", database = "test", level = "editor" }

[test]
reason = "Setup for a_permission_derives_a_computed_field_under_its_auth_limit.surql, defined by a database editor so every field carries the editor's AUTH LIMIT."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: doc:pub, status: 'public' }, { id: doc:sec, status: 'secret' }]"

*/

DEFINE FUNCTION fn::is_secret($status: string) { RETURN $status = 'secret'; };
DEFINE TABLE flag SCHEMALESS PERMISSIONS NONE;
DEFINE TABLE doc SCHEMALESS
    PERMISSIONS FOR select WHERE hidden != true AND flagged != true, FOR create, update FULL;
DEFINE FIELD hidden ON doc COMPUTED fn::is_secret(status);
DEFINE FIELD flagged ON doc COMPUTED (SELECT VALUE id FROM ONLY flag WHERE target = $parent.id LIMIT 1) != NONE;
INSERT INTO doc [{ id: doc:pub, status: 'public' }, { id: doc:sec, status: 'secret' }] RETURN id, status;
