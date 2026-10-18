/**
[env]
imports = ["reproductions/7464_auth_id_when_the_session_cannot_read_its_record_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "`$auth` is the signed-in record's id. Reading a field of it is a record fetch judged by the table's select permission, so with `PERMISSIONS NONE` on the session's own table `$auth.id` and `$auth.name` read as NONE while `$auth` and `record::id($auth)` still answer."
issue = 7464

[[test.results]]
value = "person:bob"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'bob'"
*/

RETURN $auth;
RETURN $auth.id;
RETURN $auth.name;
RETURN record::id($auth);
