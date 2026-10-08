/**
[env]
namespace = true
database = true
auth = { namespace = "test", database = "test", level = "editor" }

[test]
reason = "Setup for a_write_gate_derives_computed_fields_under_their_auth_limit.surql, defined by a database editor so every field carries the editor's AUTH LIMIT."

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: doc:pub }, { id: doc:sec }, { id: doc:tg }]"

[[test.results]]
value = "[{ id: tag:1 }]"
*/
DEFINE FUNCTION fn::is_secret($status: string) { RETURN $status = 'secret'; };
DEFINE TABLE tag SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE flag SCHEMALESS PERMISSIONS NONE;
DEFINE TABLE doc SCHEMALESS
    PERMISSIONS FOR select FULL, FOR create, update, delete WHERE hidden != true AND tagged != true AND (flagged = true OR true);
DEFINE FIELD hidden ON doc COMPUTED fn::is_secret(status);
DEFINE FIELD flagged ON doc COMPUTED (SELECT VALUE id FROM ONLY flag WHERE target = $parent.id LIMIT 1) != NONE;
DEFINE FIELD tagged ON doc COMPUTED (SELECT VALUE id FROM ONLY tag WHERE target = $parent.id LIMIT 1) != NONE;
INSERT INTO doc [{ id: doc:pub, status: 'public' }, { id: doc:sec, status: 'secret' }, { id: doc:tg, status: 'public' }] RETURN id;
INSERT INTO tag { id: tag:1, target: doc:tg } RETURN id;
